# CI-style checks for tpt-replicate. Run from the repository root:
#   powershell -ExecutionPolicy Bypass -File scripts/ci.ps1
$ErrorActionPreference = 'Stop'

function Invoke-Step {
    param([string]$Name, [scriptblock]$Command)
    Write-Host "==> $Name" -ForegroundColor Cyan
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "step failed: $Name (exit $LASTEXITCODE)" }
}

Invoke-Step 'format' { cargo fmt --check }
Invoke-Step 'build, no default features' { cargo build --no-default-features }
Invoke-Step 'build, all features and targets' { cargo build --all-features --all-targets }
Invoke-Step 'test, no default features' { cargo test }
Invoke-Step 'test, all features' { cargo test --all-features }
Invoke-Step 'clippy, warnings are errors' { cargo clippy --all-targets --all-features -- -D warnings }
Invoke-Step 'rustdoc, warnings are errors' {
    $env:RUSTDOCFLAGS = '-D warnings'
    try { cargo doc --no-deps --all-features } finally { Remove-Item Env:RUSTDOCFLAGS -ErrorAction SilentlyContinue }
}
Invoke-Step 'no_std bare-metal build (thumbv7em-none-eabihf)' {
    cargo build --no-default-features --target thumbv7em-none-eabihf
}

Write-Host '==> dependency audit (normal dependencies must be empty)' -ForegroundColor Cyan
$tree = cargo tree -e normal --no-default-features --prefix none
if ($LASTEXITCODE -ne 0) { throw 'cargo tree failed' }
$deps = @($tree | Where-Object { $_ -and ($_ -notmatch '^tpt-replicate ') })
if ($deps.Count -ne 0) { throw "unexpected normal dependencies: $($deps -join ', ')" }

Write-Host 'all checks passed' -ForegroundColor Green
