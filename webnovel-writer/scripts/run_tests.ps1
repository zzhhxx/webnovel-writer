param(
    [ValidateSet("smoke", "full")]
    [string]$Mode = "smoke",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
} else {
    $ProjectRoot = (Resolve-Path $ProjectRoot).Path
}

Set-Location $ProjectRoot

$pythonExe = ""
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

$scriptRootCandidates = @(
    (Join-Path $ProjectRoot "webnovel-writer\\scripts"),
    (Join-Path $ProjectRoot "scripts"),
    (Join-Path $ProjectRoot ".claude\\scripts")
)
$scriptRoot = $null
foreach ($candidate in $scriptRootCandidates) {
    if (Test-Path $candidate) {
        $scriptRoot = $candidate
        break
    }
}
if (-not $scriptRoot) {
    throw "Script root not found. Tried: webnovel-writer/scripts, scripts, .claude/scripts."
}

$testsRoot = Join-Path $scriptRoot "data_modules\\tests"
if (-not (Test-Path $testsRoot)) {
    throw "Tests root not found: $testsRoot"
}

$tmpRoot = Join-Path $ProjectRoot ".tmp\\pytest"
New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null

$origTMP = $env:TMP
$origTEMP = $env:TEMP
$env:TMP = $tmpRoot
$env:TEMP = $tmpRoot
$env:PYTHONPATH = $scriptRoot

# Unique basetemp to avoid stale lock/permission conflicts on Windows.
$runId = Get-Date -Format "yyyyMMdd_HHmmssfff"
$baseTemp = Join-Path $tmpRoot ("run-" + $Mode + "-" + $runId)
$useIsolatedTemp = $true

$ignoreArgs = @()
$knownBadDirs = @(
    "localtmp",
    "permtemp",
    "runtime_pytest",
    "tmphkqtr09m",
    "tmpx",
    ".pytest_tmp2"
)
foreach ($dirName in $knownBadDirs) {
    $badDir = Join-Path $testsRoot $dirName
    if (Test-Path $badDir -ErrorAction SilentlyContinue) {
        $ignoreArgs += @("--ignore", $badDir)
    }
}

Write-Host "ProjectRoot: $ProjectRoot"
Write-Host "TMP/TEMP: $tmpRoot"
Write-Host "Mode: $Mode"
Write-Host "Python: $pythonExe"
Write-Host "ScriptRoot: $scriptRoot"
Write-Host "TestsRoot: $testsRoot"

# Precheck temp dir permissions. Some Python builds create inaccessible temp dirs.
@'
import tempfile
from pathlib import Path
import sys

try:
    d = Path(tempfile.mkdtemp(prefix="webnovel_writer_pytest_"))
    list(d.iterdir())
    (d / "probe.txt").write_text("ok", encoding="utf-8")
except Exception as exc:
    print(f"PYTEST_TMPDIR_PRECHECK_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    raise
'@ | Set-Variable -Name precheckScript
$oldEAP = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$precheckOutput = $precheckScript | & $pythonExe - 2>&1
$precheckExitCode = $LASTEXITCODE
$ErrorActionPreference = $oldEAP

if ($precheckExitCode -ne 0) {
    Write-Host ""
    Write-Host "Warning: temp dir precheck failed."
    if ($precheckOutput) {
        $precheckOutput | ForEach-Object { Write-Host $_ }
    }
    Write-Host "Fallback: use system temp dir and do not force --basetemp."
    $useIsolatedTemp = $false
    if ($null -ne $origTMP -and "$origTMP" -ne "") {
        $env:TMP = $origTMP
    } else {
        Remove-Item Env:TMP -ErrorAction SilentlyContinue
    }
    if ($null -ne $origTEMP -and "$origTEMP" -ne "") {
        $env:TEMP = $origTEMP
    } else {
        Remove-Item Env:TEMP -ErrorAction SilentlyContinue
    }
}

if ($Mode -eq "smoke") {
    $smokeTests = @(
        (Join-Path $testsRoot "test_extract_chapter_context.py"),
        (Join-Path $testsRoot "test_rag_adapter.py")
    )
    foreach ($t in $smokeTests) {
        if (-not (Test-Path $t)) {
            throw "Smoke test file not found: $t"
        }
    }
    $smokeArgs = @("-m", "pytest", "-q") + $smokeTests + @("--no-cov", "-p", "no:cacheprovider") + $ignoreArgs
    if ($useIsolatedTemp) {
        $smokeArgs += @("--basetemp", $baseTemp)
    }
    & $pythonExe @smokeArgs
    exit $LASTEXITCODE
}

$fullArgs = @("-m", "pytest", "-q", $testsRoot, "--no-cov", "-p", "no:cacheprovider") + $ignoreArgs
if ($useIsolatedTemp) {
    $fullArgs += @("--basetemp", $baseTemp)
}
& $pythonExe @fullArgs
exit $LASTEXITCODE
