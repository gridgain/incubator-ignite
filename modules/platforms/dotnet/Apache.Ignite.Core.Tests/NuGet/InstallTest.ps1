param (
    [string]$Devenv="${Env:ProgramFiles(x86)}\Microsoft Visual Studio\2017\Community\Common7\IDE\devenv.com"
)

$toolsPath = Join-Path $PSScriptRoot (Join-Path ".." (Join-Path ".." (Join-Path "Apache.Ignite.Core" "NuGet")))
$solProps = @{
    FullName = Join-Path $PSScriptRoot "App.csproj"
}
$dteProps = @{
    Solution = New-Object psobject -Property $solProps
}
$dte = New-Object psobject -Property $dteProps

&"$toolsPath\Install.ps1" -installPath $PSScriptRoot -toolsPath $toolsPath





#&$Devenv $PSScriptRoot\App.csproj /Build
#$Msbuild="${Env:SystemRoot}\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe"