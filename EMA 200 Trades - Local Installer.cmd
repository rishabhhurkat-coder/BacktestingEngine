<# : batch
@echo off
setlocal
set "INSTALLER_SELF=%~f0"
powershell -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -Command ^
 "$self = '%~f0'; " ^
 "$lines = Get-Content -LiteralPath $self; " ^
 "$marker = [Array]::IndexOf($lines, '#<POWERSHELL>#'); " ^
 "if ($marker -lt 0) { throw 'Installer payload marker not found.' }; " ^
 "$script = ($lines[($marker + 1)..($lines.Length - 1)] -join [Environment]::NewLine); " ^
 "$tempScript = Join-Path $env:TEMP ('ema200-bootstrap-' + [Guid]::NewGuid().ToString('N') + '.ps1'); " ^
 "[IO.File]::WriteAllText($tempScript, $script, [Text.UTF8Encoding]::new($false)); " ^
 "& powershell -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File $tempScript; " ^
 "$code = $LASTEXITCODE; " ^
 "Remove-Item -LiteralPath $tempScript -Force -ErrorAction SilentlyContinue; " ^
 "exit $code"
exit /b %errorlevel%
#>
#<POWERSHELL>#
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$AppName = 'EMA 200 Trades - Local'
$GithubRepo = 'rishabhhurkat-coder/BacktestingEngine'
$ReleaseAssetName = 'EMA-200-Trades-Local-package.zip'
$InstallerSource = $env:INSTALLER_SELF
$InstallerDrive = if ($InstallerSource) { Split-Path -Qualifier $InstallerSource } else { '' }
$TargetRoot = if ($InstallerDrive -and (Test-Path $InstallerDrive)) { $InstallerDrive } elseif (Test-Path 'D:\') { 'D:\' } else { $env:SystemDrive + '\' }
$InstallDir = Join-Path $TargetRoot $AppName
$DesktopDir = [Environment]::GetFolderPath('Desktop')
$ShortcutPath = Join-Path $DesktopDir ($AppName + '.lnk')
$TempDir = Join-Path ([IO.Path]::GetTempPath()) ('ema200-bootstrap-' + [Guid]::NewGuid().ToString('N'))
$ZipPath = Join-Path $TempDir 'package.zip'
$ExtractDir = Join-Path $TempDir 'extract'

$Form = New-Object System.Windows.Forms.Form
$Form.Text = 'Installing EMA 200 Trades - Local'
$Form.StartPosition = 'CenterScreen'
$Form.Size = New-Object System.Drawing.Size(560, 170)
$Form.FormBorderStyle = 'FixedDialog'
$Form.MaximizeBox = $false
$Form.MinimizeBox = $false
$Form.TopMost = $true

$TitleLabel = New-Object System.Windows.Forms.Label
$TitleLabel.Text = 'EMA 200 Trades - Local'
$TitleLabel.Font = New-Object System.Drawing.Font('Segoe UI', 14, [System.Drawing.FontStyle]::Bold)
$TitleLabel.AutoSize = $true
$TitleLabel.Location = New-Object System.Drawing.Point(18, 18)
$Form.Controls.Add($TitleLabel)

$StatusLabel = New-Object System.Windows.Forms.Label
$StatusLabel.Text = 'Preparing installer...'
$StatusLabel.Font = New-Object System.Drawing.Font('Segoe UI', 10)
$StatusLabel.AutoSize = $true
$StatusLabel.Location = New-Object System.Drawing.Point(18, 55)
$Form.Controls.Add($StatusLabel)

$Progress = New-Object System.Windows.Forms.ProgressBar
$Progress.Minimum = 0
$Progress.Maximum = 100
$Progress.Value = 0
$Progress.Style = 'Continuous'
$Progress.Size = New-Object System.Drawing.Size(510, 22)
$Progress.Location = New-Object System.Drawing.Point(18, 80)
$Form.Controls.Add($Progress)

$DetailLabel = New-Object System.Windows.Forms.Label
$DetailLabel.Text = ''
$DetailLabel.Font = New-Object System.Drawing.Font('Segoe UI', 9)
$DetailLabel.AutoSize = $true
$DetailLabel.Location = New-Object System.Drawing.Point(18, 112)
$Form.Controls.Add($DetailLabel)

function Update-Ui([int]$Percent, [string]$Status, [string]$Detail = '') {
    $Progress.Value = [Math]::Max(0, [Math]::Min(100, $Percent))
    $StatusLabel.Text = $Status
    $DetailLabel.Text = $Detail
    $Form.Refresh()
}

function Get-PythonCommand {
    $candidates = @(
        @{ File = 'py'; Args = @('-3.12') },
        @{ File = 'py'; Args = @('-3') },
        @{ File = 'python'; Args = @() }
    )

    foreach ($candidate in $candidates) {
        try {
            $null = & $candidate.File @($candidate.Args + @('-c', 'import sys; print(sys.executable)')) 2>$null
            return @($candidate.File) + $candidate.Args
        } catch {}
    }

    return $null
}

function Ensure-PythonInstalled {
    $pythonCmd = Get-PythonCommand
    if ($pythonCmd) {
        return $pythonCmd
    }

    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Update-Ui 52 'Installing Python...' 'Python 3.12'
        winget install --id Python.Python.3.12 -e --accept-package-agreements --accept-source-agreements | Out-Null
        $env:Path = [System.Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path', 'User')
        $pythonCmd = Get-PythonCommand
        if ($pythonCmd) {
            return $pythonCmd
        }
    }

    throw 'Python 3 was not found. Please install Python 3 and run this installer again.'
}

function Get-LatestReleaseAsset {
    Update-Ui 8 'Checking GitHub release...' $GithubRepo
    $headers = @{ 'User-Agent' = 'EMA-200-Trades-Local-Installer'; 'Accept' = 'application/vnd.github+json' }
    $release = Invoke-RestMethod -Uri ("https://api.github.com/repos/rishabhhurkat-coder/BacktestingEngine/releases/latest") -Headers $headers
    $asset = $release.assets | Where-Object { $_.name -eq $ReleaseAssetName } | Select-Object -First 1
    if (-not $asset) {
        $asset = $release.assets | Where-Object { $_.name -like '*.zip' } | Select-Object -First 1
    }
    if (-not $asset) {
        throw 'No update package asset was found in the latest GitHub release.'
    }
    return $asset.browser_download_url
}

function Download-ReleaseAsset([string]$AssetUrl, [string]$DestinationPath) {
    Update-Ui 15 'Downloading package...' $ReleaseAssetName
    $request = [System.Net.HttpWebRequest]::Create($AssetUrl)
    $request.UserAgent = 'EMA-200-Trades-Local-Installer'
    $response = $request.GetResponse()
    $totalBytes = $response.ContentLength
    $stream = $response.GetResponseStream()
    $fileStream = [System.IO.File]::Open($DestinationPath, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write)
    try {
        $buffer = New-Object byte[] (262144)
        $readTotal = 0L
        while (($read = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            $fileStream.Write($buffer, 0, $read)
            $readTotal += $read
            if ($totalBytes -gt 0) {
                $percent = 15 + [int](($readTotal / $totalBytes) * 30)
                Update-Ui $percent 'Downloading package...' ("0 KB / 1 KB" -f [int]($readTotal / 1KB), [int]($totalBytes / 1KB))
            }
        }
    } finally {
        $fileStream.Dispose()
        $stream.Dispose()
        $response.Dispose()
    }
}

function New-DesktopShortcut([string]$InstallDir) {
    $iconPath = Join-Path $InstallDir 'assets\ema_200_trades_local.ico'
    $targetPath = Join-Path $InstallDir 'Run EMA 200 Trades - Local.bat'
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($ShortcutPath)
    $shortcut.TargetPath = $targetPath
    $shortcut.WorkingDirectory = $InstallDir
    if (Test-Path $iconPath) {
        $shortcut.IconLocation = "$iconPath,0"
    }
    $shortcut.Save()
}

$installFailed = $false
try {
    Update-Ui 2 'Preparing installer...' $TargetRoot
    New-Item -ItemType Directory -Path $TempDir -Force | Out-Null
    New-Item -ItemType Directory -Path $ExtractDir -Force | Out-Null

    $assetUrl = Get-LatestReleaseAsset
    Download-ReleaseAsset -AssetUrl $assetUrl -DestinationPath $ZipPath

    Update-Ui 48 'Extracting package...' $ReleaseAssetName
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    Expand-Archive -Path $ZipPath -DestinationPath $ExtractDir -Force
    $rootEntries = Get-ChildItem -LiteralPath $ExtractDir
    $sourceDir = if ($rootEntries.Count -eq 1 -and $rootEntries[0].PSIsContainer) { $rootEntries[0].FullName } else { $ExtractDir }
    Get-ChildItem -LiteralPath $sourceDir | ForEach-Object {
        $destinationPath = Join-Path $InstallDir $_.Name
        if ($_.PSIsContainer -and $_.Name -eq 'Main Folder' -and (Test-Path $destinationPath)) {
            return
        }
        Copy-Item -Path $_.FullName -Destination $destinationPath -Recurse -Force
    }

    Update-Ui 58 'Finding Python...' ''
    $pythonCmd = Ensure-PythonInstalled
    $pythonArgs = @()
    if ($pythonCmd.Length -gt 1) {
        $pythonArgs = $pythonCmd[1..($pythonCmd.Length - 1)]
    }

    Update-Ui 68 'Creating virtual environment...' ''
    & $pythonCmd[0] @($pythonArgs + @('-m', 'venv', (Join-Path $InstallDir '.venv'))) | Out-Null
    $venvPython = Join-Path $InstallDir '.venv\Scripts\python.exe'
    if (-not (Test-Path $venvPython)) {
        throw 'Virtual environment creation failed.'
    }

    Update-Ui 80 'Installing Python packages...' 'requirements.txt'
    & $venvPython -m pip install --upgrade pip | Out-Null
    & $venvPython -m pip install -r (Join-Path $InstallDir 'requirements.txt') | Out-Null

    Update-Ui 94 'Creating desktop shortcut...' $ShortcutPath
    New-DesktopShortcut -InstallDir $InstallDir

    Update-Ui 100 'Installation complete' $InstallDir
    Start-Sleep -Milliseconds 700
    $runLauncher = Join-Path $InstallDir 'Run EMA 200 Trades - Local.bat'
    if (Test-Path $runLauncher) {
        Start-Process -FilePath $runLauncher -WorkingDirectory $InstallDir
    }
} catch {
    $installFailed = $true
    [System.Windows.Forms.MessageBox]::Show($_.Exception.Message, 'Installation failed', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
} finally {
    if (Test-Path $TempDir) {
        Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
    $Form.Close()
}
