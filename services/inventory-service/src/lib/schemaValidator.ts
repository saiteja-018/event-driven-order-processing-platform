import fs from 'fs';
import path from 'path';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';

let schemaDir = process.env.SCHEMA_DIR || '';
if (!schemaDir) {
	const pathsToTry = [
		path.resolve(__dirname, '../../shared/schemas'),
		path.resolve(__dirname, '../../../shared/schemas'),
		path.resolve(__dirname, '../../../../shared/schemas')
	];
	for (const p of pathsToTry) {
		if (fs.existsSync(p)) {
			schemaDir = p;
			break;
		}
	}
	if (!schemaDir) {
		schemaDir = pathsToTry[0];
	}
}
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);

const schemaCache: Record<string, any> = {};
const validatorCache: Record<string, any> = {};

function loadSchema(topic: string) {
	if (schemaCache[topic]) return schemaCache[topic];
	const filePath = path.join(schemaDir, `${topic}.schema.json`);
	if (!fs.existsSync(filePath)) return null;
	const schema = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
	schemaCache[topic] = schema;
	return schema;
}

export function validateEvent(topic: string, payload: any) {
	const schema = loadSchema(topic);
	if (!schema) return { valid: false, errors: [`schema_not_found:${topic}`] };
	if (!validatorCache[topic]) validatorCache[topic] = ajv.compile(schema);
	const validate = validatorCache[topic];
	const valid = validate(payload);
	return { valid, errors: validate.errors || [] };
}
