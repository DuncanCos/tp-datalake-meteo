# Capture ~5 minutes de meteo temps reel Open-Meteo (1 poll / 30s, 10 polls)
# 10 villes francaises, sortie JSON Lines (1 ligne par ville par poll)
$ErrorActionPreference = "Stop"

# lat/lon en chaines : evite le formatage culture fr (virgule decimale) qui casse l'URL
$cities = @(
    @{name="Paris";      dept="75"; lat="48.8566"; lon="2.3522"},
    @{name="Lyon";       dept="69"; lat="45.7640"; lon="4.8357"},
    @{name="Marseille";  dept="13"; lat="43.2965"; lon="5.3698"},
    @{name="Toulouse";   dept="31"; lat="43.6047"; lon="1.4442"},
    @{name="Nice";       dept="06"; lat="43.7102"; lon="7.2620"},
    @{name="Nantes";     dept="44"; lat="47.2184"; lon="-1.5536"},
    @{name="Strasbourg"; dept="67"; lat="48.5734"; lon="7.7521"},
    @{name="Bordeaux";   dept="33"; lat="44.8378"; lon="-0.5792"},
    @{name="Lille";      dept="59"; lat="50.6292"; lon="3.0573"},
    @{name="Rennes";     dept="35"; lat="48.1173"; lon="-1.6778"}
)

$lats = ($cities | ForEach-Object { $_.lat }) -join ","
$lons = ($cities | ForEach-Object { $_.lon }) -join ","
$vars = "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
$url = "https://api.open-meteo.com/v1/forecast?latitude=$lats&longitude=$lons&current=$vars&timezone=UTC"

$outFile = Join-Path $PSScriptRoot "..\data_sample\openmeteo\current_capture.jsonl"

for ($i = 1; $i -le 10; $i++) {
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    try {
        $resp = Invoke-RestMethod -Uri $url -TimeoutSec 20
        for ($j = 0; $j -lt $cities.Count; $j++) {
            $rec = [ordered]@{
                ingested_at = $ts
                poll        = $i
                city        = $cities[$j].name
                dept        = $cities[$j].dept
                latitude    = $resp[$j].latitude
                longitude   = $resp[$j].longitude
                elevation   = $resp[$j].elevation
                current     = $resp[$j].current
            }
            ($rec | ConvertTo-Json -Compress -Depth 5) | Add-Content -Encoding utf8 $outFile
        }
        Write-Output "poll $i/10 ok at $ts"
    } catch {
        Write-Output "poll $i/10 FAILED: $($_.Exception.Message)"
    }
    if ($i -lt 10) { Start-Sleep -Seconds 30 }
}
Write-Output "capture done"
