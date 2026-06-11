#!/usr/bin/env bash
set -uo pipefail

echo "LocalStack init script starting"

ENDPOINT="http://localhost:4566"
AWS_CLI=(awslocal)

# wait for LocalStack to be ready
for i in $(seq 1 40); do
  if "${AWS_CLI[@]}" sts get-caller-identity > /dev/null 2>&1; then
    echo "LocalStack is ready"
    break
  fi
  echo "Waiting for LocalStack... ($i)"
  sleep 3
done

echo "Creating SQS queues"

# Create DLQ first so we can reference its ARN
"${AWS_CLI[@]}" sqs create-queue \
  --queue-name payment-processing-dlq \
  --attributes MessageRetentionPeriod=345600,VisibilityTimeout=30 \
  > /dev/null 2>&1 || true

PAYMENT_DLQ_ARN="arn:aws:sqs:us-east-1:000000000000:payment-processing-dlq"

"${AWS_CLI[@]}" sqs create-queue \
  --queue-name payment-processing-queue \
  --attributes '{"VisibilityTimeout":"30","MessageRetentionPeriod":"86400","RedrivePolicy":"{\"maxReceiveCount\":\"3\",\"deadLetterTargetArn\":\"'"${PAYMENT_DLQ_ARN}"'\"}"}' \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" sqs create-queue \
  --queue-name notification-email-queue \
  --attributes VisibilityTimeout=60,MessageRetentionPeriod=86400 \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" sqs create-queue \
  --queue-name notification-sms-queue \
  --attributes VisibilityTimeout=60,MessageRetentionPeriod=86400 \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" sqs create-queue \
  --queue-name reservation-expiry-queue \
  --attributes VisibilityTimeout=30,MessageRetentionPeriod=86400 \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" sqs create-queue \
  --queue-name order-webhook-queue \
  --attributes VisibilityTimeout=45,MessageRetentionPeriod=86400 \
  > /dev/null 2>&1 || true

echo "Creating SNS topics"
"${AWS_CLI[@]}" sns create-topic --name order-lifecycle-topic > /dev/null 2>&1 || true
"${AWS_CLI[@]}" sns create-topic --name payment-events-topic > /dev/null 2>&1 || true
"${AWS_CLI[@]}" sns create-topic --name inventory-alerts-topic > /dev/null 2>&1 || true

ORDER_LIFECYCLE_ARN="arn:aws:sns:us-east-1:000000000000:order-lifecycle-topic"

echo "Subscribing SQS queues to SNS topics"

EMAIL_QUEUE_ARN="arn:aws:sqs:us-east-1:000000000000:notification-email-queue"
SMS_QUEUE_ARN="arn:aws:sqs:us-east-1:000000000000:notification-sms-queue"
WEBHOOK_QUEUE_ARN="arn:aws:sqs:us-east-1:000000000000:order-webhook-queue"

# Subscribe email and sms queues to order-lifecycle-topic (no filter)
"${AWS_CLI[@]}" sns subscribe \
  --topic-arn "${ORDER_LIFECYCLE_ARN}" \
  --protocol sqs \
  --notification-endpoint "${EMAIL_QUEUE_ARN}" \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" sns subscribe \
  --topic-arn "${ORDER_LIFECYCLE_ARN}" \
  --protocol sqs \
  --notification-endpoint "${SMS_QUEUE_ARN}" \
  > /dev/null 2>&1 || true

# Subscribe webhook queue with filter policy for ORDER_COMPLETED and ORDER_CANCELLED
"${AWS_CLI[@]}" sns subscribe \
  --topic-arn "${ORDER_LIFECYCLE_ARN}" \
  --protocol sqs \
  --notification-endpoint "${WEBHOOK_QUEUE_ARN}" \
  --attributes 'FilterPolicy={"eventType":["ORDER_COMPLETED","ORDER_CANCELLED"]}' \
  > /dev/null 2>&1 || true

echo "Packaging and deploying Lambda functions"

LAMBDA_INIT_DIR="/etc/localstack/init/ready.d/lambdas"
LAMBDA_WORK_DIR="/tmp/lambda-work"
mkdir -p "${LAMBDA_WORK_DIR}"

DB_URL="${DATABASE_URL:-postgres://postgres:postgres@postgres:5432/app}"
KAFKA_BROKERS_VAL="${KAFKA_BROKERS:-kafka:9092}"
LOCALSTACK_URL_VAL="${LOCALSTACK_URL:-http://localstack:4566}"
LAMBDA_ENV="Variables={DATABASE_URL=${DB_URL},KAFKA_BROKERS=${KAFKA_BROKERS_VAL},LOCALSTACK_URL=${LOCALSTACK_URL_VAL}}"

for FUNC_NAME in process-payment expire-reservations compute-hourly-metrics; do
  SRC_DIR="${LAMBDA_INIT_DIR}/${FUNC_NAME}"
  WORK_DIR="${LAMBDA_WORK_DIR}/${FUNC_NAME}"
  ZIP_FILE="/tmp/${FUNC_NAME}.zip"

  if [ ! -f "${SRC_DIR}/handler.js" ]; then
    echo "Warning: ${FUNC_NAME}/handler.js not found, skipping"
    continue
  fi

  mkdir -p "${WORK_DIR}"
  cp "${SRC_DIR}/handler.js" "${WORK_DIR}/handler.js"

  # Copy package.json if present (for dependencies)
  if [ -f "${SRC_DIR}/package.json" ]; then
    cp "${SRC_DIR}/package.json" "${WORK_DIR}/package.json"
    cd "${WORK_DIR}" && npm install --production --silent 2>/dev/null || true
    cd /
  fi

  # Zip everything
  rm -f "${ZIP_FILE}"
  cd "${WORK_DIR}" && zip -r "${ZIP_FILE}" . > /dev/null 2>&1
  cd /

  # Check if function exists
  if "${AWS_CLI[@]}" lambda get-function --function-name "${FUNC_NAME}" > /dev/null 2>&1; then
    echo "Updating Lambda ${FUNC_NAME}"
    "${AWS_CLI[@]}" lambda update-function-code \
      --function-name "${FUNC_NAME}" \
      --zip-file "fileb://${ZIP_FILE}" > /dev/null 2>&1 || true
    "${AWS_CLI[@]}" lambda update-function-configuration \
      --function-name "${FUNC_NAME}" \
      --environment "${LAMBDA_ENV}" > /dev/null 2>&1 || true
  else
    echo "Creating Lambda ${FUNC_NAME}"
    "${AWS_CLI[@]}" lambda create-function \
      --function-name "${FUNC_NAME}" \
      --runtime nodejs20.x \
      --handler handler.handler \
      --zip-file "fileb://${ZIP_FILE}" \
      --role arn:aws:iam::000000000000:role/lambda-role \
      --environment "${LAMBDA_ENV}" \
      --timeout 60 \
      > /dev/null 2>&1 || true
  fi
done

echo "Creating event source mapping for payment-processing-queue -> process-payment"
PAYMENT_QUEUE_ARN="arn:aws:sqs:us-east-1:000000000000:payment-processing-queue"

EXISTING_MAPPING=$("${AWS_CLI[@]}" lambda list-event-source-mappings \
  --function-name process-payment \
  --query 'EventSourceMappings | length(@)' --output text 2>/dev/null || echo "0")

if [ "${EXISTING_MAPPING}" = "0" ] || [ -z "${EXISTING_MAPPING}" ]; then
  "${AWS_CLI[@]}" lambda create-event-source-mapping \
    --function-name process-payment \
    --event-source-arn "${PAYMENT_QUEUE_ARN}" \
    --batch-size 1 \
    > /dev/null 2>&1 || true
fi

echo "Creating EventBridge scheduled rules"

"${AWS_CLI[@]}" events put-rule \
  --name expire-reservations-rule \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED \
  > /dev/null 2>&1 || true

"${AWS_CLI[@]}" events put-rule \
  --name compute-hourly-metrics-rule \
  --schedule-expression "cron(0 * * * ? *)" \
  --state ENABLED \
  > /dev/null 2>&1 || true

for FUNC_NAME in expire-reservations compute-hourly-metrics; do
  RULE_NAME="${FUNC_NAME}-rule"
  LAMBDA_ARN="arn:aws:lambda:us-east-1:000000000000:function:${FUNC_NAME}"

  if [ -n "${LAMBDA_ARN}" ] && [ "${LAMBDA_ARN}" != "None" ]; then
    "${AWS_CLI[@]}" events put-targets \
      --rule "${RULE_NAME}" \
      --targets "Id=1,Arn=${LAMBDA_ARN}" \
      > /dev/null 2>&1 || true

    "${AWS_CLI[@]}" lambda add-permission \
      --function-name "${FUNC_NAME}" \
      --statement-id "${FUNC_NAME}-events-permission" \
      --action 'lambda:InvokeFunction' \
      --principal events.amazonaws.com \
      --source-arn "arn:aws:events:us-east-1:000000000000:rule/${RULE_NAME}" \
      > /dev/null 2>&1 || true
  fi
done

echo "LocalStack init complete"
exit 0
