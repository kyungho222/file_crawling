[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BastionHost,

    [Parameter(Mandatory = $true)]
    [string]$BastionUser,

    [string]$IdentityFile,
    [int]$SshPort = 22,
    [string]$MariaHost = '10.20.20.10',
    [int]$MariaPort = 3306,
    [string]$RedisHost = '192.168.1.14',
    [int]$RedisPort = 6379,
    [int]$LocalMariaPort = 13306,
    [int]$LocalRedisPort = 16379
)

$arguments = @(
    '-N',
    '-p', $SshPort,
    '-L', "${LocalMariaPort}:${MariaHost}:${MariaPort}",
    '-L', "${LocalRedisPort}:${RedisHost}:${RedisPort}"
)
if ($IdentityFile) {
    $arguments += @('-i', $IdentityFile)
}
$arguments += "${BastionUser}@${BastionHost}"

Write-Host "Opening MariaDB localhost:${LocalMariaPort} and Redis localhost:${LocalRedisPort} tunnel."
& ssh.exe @arguments