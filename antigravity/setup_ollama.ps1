# Antigravity Ollama Setup Script for Windows
# This script installs Ollama and configures it for Antigravity

Write-Host "=== Antigravity Ollama Setup ===" -ForegroundColor Cyan
Write-Host ""

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "WARNING: Not running as Administrator. Some operations may fail." -ForegroundColor Yellow
    Write-Host "Consider running: Start-Process powershell -Verb RunAs -ArgumentList '-File $PSCommandPath'" -ForegroundColor Yellow
    Write-Host ""
}

# Step 1: Check if Ollama is already installed
Write-Host "[1/5] Checking for existing Ollama installation..." -ForegroundColor Green
$ollamaPath = Get-Command ollama -ErrorAction SilentlyContinue

if ($ollamaPath) {
    Write-Host "✓ Ollama is already installed at: $($ollamaPath.Source)" -ForegroundColor Green
    $version = & ollama --version 2>&1
    Write-Host "  Version: $version" -ForegroundColor Gray
} else {
    Write-Host "✗ Ollama not found. Installing..." -ForegroundColor Yellow
    
    # Download Ollama installer
    $installerUrl = "https://ollama.com/download/OllamaSetup.exe"
    $installerPath = "$env:TEMP\OllamaSetup.exe"
    
    Write-Host "  Downloading from $installerUrl..." -ForegroundColor Gray
    try {
        Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath -UseBasicParsing
        Write-Host "  Download complete." -ForegroundColor Gray
        
        # Run installer
        Write-Host "  Running installer..." -ForegroundColor Gray
        Start-Process -FilePath $installerPath -Wait -NoNewWindow
        
        # Verify installation
        $ollamaPath = Get-Command ollama -ErrorAction SilentlyContinue
        if ($ollamaPath) {
            Write-Host "✓ Ollama installed successfully!" -ForegroundColor Green
        } else {
            Write-Host "✗ Installation failed. Please install manually from https://ollama.com/download" -ForegroundColor Red
            exit 1
        }
    } catch {
        Write-Host "✗ Download failed: $_" -ForegroundColor Red
        Write-Host "  Please install manually from https://ollama.com/download" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host ""

# Step 2: Check if Ollama service is running
Write-Host "[2/5] Checking Ollama service status..." -ForegroundColor Green
try {
    $response = Invoke-WebRequest -Uri "http://localhost:11434/api/tags" -Method GET -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
    Write-Host "✓ Ollama service is running on http://localhost:11434" -ForegroundColor Green
} catch {
    Write-Host "✗ Ollama service is not responding. Starting service..." -ForegroundColor Yellow
    
    # Try to start Ollama
    Start-Process "ollama" -ArgumentList "serve" -WindowStyle Hidden
    Start-Sleep -Seconds 5
    
    # Verify again
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:11434/api/tags" -Method GET -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        Write-Host "✓ Ollama service started successfully!" -ForegroundColor Green
    } catch {
        Write-Host "✗ Failed to start Ollama service. Please run 'ollama serve' manually." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""

# Step 3: Check for installed models
Write-Host "[3/5] Checking for installed models..." -ForegroundColor Green
$models = & ollama list 2>&1 | Select-String -Pattern "mixtral|llama3"

if ($models) {
    Write-Host "✓ Found existing models:" -ForegroundColor Green
    & ollama list
} else {
    Write-Host "✗ No suitable models found." -ForegroundColor Yellow
}

Write-Host ""

# Step 4: Pull recommended model
Write-Host "[4/5] Pulling recommended model (Mixtral 8x7b)..." -ForegroundColor Green
Write-Host "  This may take 10-20 minutes depending on your internet speed..." -ForegroundColor Gray
Write-Host "  Model size: ~26GB" -ForegroundColor Gray
Write-Host ""

$pullMixtral = Read-Host "Pull Mixtral 8x7b? (Y/n)"
if ($pullMixtral -ne 'n' -and $pullMixtral -ne 'N') {
    Write-Host "  Pulling mixtral:8x7b..." -ForegroundColor Gray
    & ollama pull mixtral:8x7b
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Mixtral 8x7b pulled successfully!" -ForegroundColor Green
    } else {
        Write-Host "✗ Failed to pull Mixtral. You can try manually: ollama pull mixtral:8x7b" -ForegroundColor Red
    }
} else {
    Write-Host "  Skipped. Checking for Llama3 8b (faster, smaller alternative)..." -ForegroundColor Yellow
    
    $pullLlama = Read-Host "Pull Llama3 8b instead? (Y/n)"
    if ($pullLlama -ne 'n' -and $pullLlama -ne 'N') {
        Write-Host "  Pulling llama3:8b..." -ForegroundColor Gray
        & ollama pull llama3:8b
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Llama3 8b pulled successfully!" -ForegroundColor Green
        } else {
            Write-Host "✗ Failed to pull Llama3." -ForegroundColor Red
        }
    }
}

Write-Host ""

# Step 5: Verify installation
Write-Host "[5/5] Verifying Ollama setup..." -ForegroundColor Green

# Test model inference
Write-Host "  Testing model inference..." -ForegroundColor Gray
$testPrompt = "Respond with only the word 'SUCCESS' if you can read this."
$testResult = & ollama run mixtral:8x7b $testPrompt 2>&1

if ($testResult -match "SUCCESS") {
    Write-Host "✓ Model inference test passed!" -ForegroundColor Green
} else {
    # Try Llama3 if Mixtral failed
    $testResult = & ollama run llama3:8b $testPrompt 2>&1
    if ($testResult -match "SUCCESS") {
        Write-Host "✓ Model inference test passed (using Llama3)!" -ForegroundColor Green
    } else {
        Write-Host "✗ Model inference test failed. Output:" -ForegroundColor Red
        Write-Host $testResult -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Verify Ollama is running: curl http://localhost:11434/api/tags" -ForegroundColor White
Write-Host "2. List installed models: ollama list" -ForegroundColor White
Write-Host "3. Test a model: ollama run mixtral:8x7b 'Hello, world!'" -ForegroundColor White
Write-Host "4. Configure Antigravity to use Ollama (see implementation_plan.md)" -ForegroundColor White
Write-Host ""
Write-Host "Ollama API endpoint: http://localhost:11434" -ForegroundColor Cyan
Write-Host "Recommended model: mixtral:8x7b" -ForegroundColor Cyan
Write-Host ""
