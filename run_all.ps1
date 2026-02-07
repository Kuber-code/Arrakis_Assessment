# run_all.ps1
$ErrorActionPreference = "Stop"

function Load-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf("=")
    if ($idx -lt 1) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Write-Host "== Arrakis Assessment: full pipeline =="

# Create all expected folders (safe if they already exist)
$DIRS = @(
  "data",
  "data\raw",
  "data\processed",
  "data\legacy",
  "scripts",
  "scripts\legacy",
  "figures",
  "report"
)

foreach ($d in $DIRS) {
  New-Item -ItemType Directory -Force -Path $d | Out-Null
}

# Load .env (RPC_URL, etc.)
Load-DotEnv ".env"
if (-not $env:RPC_URL -or $env:RPC_URL.Trim() -eq "") {
  throw "RPC_URL is not set. Add it to .env or set it in your environment."
}

# Create venv if missing
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
  Write-Host "Creating venv..."
  python -m venv .venv
}

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Upgrading pip..."
& $PY -m pip install --upgrade pip

Write-Host "Installing requirements..."
& $PY -m pip install -r requirements.txt

# Run scripts in order
$SCRIPTS = @(
  "scripts\00_verify_addresses.py",
  "scripts\01_univ2_pair_metadata.py",
  "scripts\02_find_migration_block_univ2.py",
  "scripts\03_confirm_migration_events_univ2.py",
  "scripts\04_write_migration_block_final.py",
  "scripts\05_univ2_slippage_pre_usd.py",
  "scripts\06_univ4_slippage_post_usd.py",
  "scripts\07_execution_quality_plots.py",
  "scripts\08_univ4_liquidity_distribution.py",
  "scripts\09a_probe_vault_interface.py",
  "scripts\09_vault_timeseries.py",
  "scripts\10_vault_performance_plots.py"
)

foreach ($s in $SCRIPTS) {
  if (-not (Test-Path $s)) { throw "Missing script: $s" }
  Write-Host "`n== Running $s =="
  & $PY $s
}

Write-Host "`n== Done =="
Write-Host "Outputs:"
Write-Host "  - data\raw\"
Write-Host "  - data\processed\"
Write-Host "  - figures\"
Write-Host "  - report\"
