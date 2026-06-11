param(
  [string]$ApiBase = 'http://localhost:3000',
  [string]$DatabaseUrl = 'postgres://postgres:postgres@localhost:5432/app'
)

Write-Host "Starting Docker Compose (detached)..."
docker compose up --build -d

function Wait-Port($computerName, $port, $timeoutSec = 120) {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $timeoutSec) {
    try {
      $tcp = New-Object System.Net.Sockets.TcpClient($computerName, $port)
      $tcp.Close()
      Write-Host "Port $port on $computerName is open."
      return $true
    } catch {}
    Start-Sleep -Seconds 2
  }
  return $false
}

Write-Host "Waiting for Postgres (5432)..."
if (-not (Wait-Port '127.0.0.1' 5432 180)) { Write-Error 'Postgres did not become available in time.'; exit 1 }
Write-Host "Waiting for LocalStack (4566)..."
if (-not (Wait-Port '127.0.0.1' 4566 180)) { Write-Error 'LocalStack did not become available in time.'; exit 1 }
Write-Host "Waiting for Kafka (29092)..."
if (-not (Wait-Port '127.0.0.1' 29092 180)) { Write-Host 'Kafka broker on 29092 may not be exposed; continuing anyway.' }

# Run migrations: prefer local psql if available, else show docker-run command
if (Get-Command psql -ErrorAction SilentlyContinue) {
  Write-Host 'psql detected locally — running migrations...'
  $files = Get-ChildItem -Path infra/migrations -Filter 'V*.sql' | Sort-Object Name
  foreach ($f in $files) {
    Write-Host "Applying $($f.Name)"
    psql $env:DATABASE_URL -f $f.FullName
  }
  Write-Host 'Applying seed.sql'
  psql $env:DATABASE_URL -f infra/migrations/seed.sql
}
else {
  Write-Host 'psql not found locally. To apply migrations you can run this command (PowerShell):'
  Write-Host "docker run --rm -v ${PWD}:/work -w /work postgres:15 psql -h host.docker.internal -U postgres -d app -f infra/migrations/V001__create_order_service_schema.sql"
  Write-Host 'Or run psql directly on your workstation. Skipping automatic migrations.'
}

Write-Host 'Running integration tests (this will install test deps)...'
Push-Location tests
npm install
npm test
Pop-Location

Write-Host 'Bringing down Docker Compose (stopping containers)...'
docker compose down

Write-Host 'Done.'