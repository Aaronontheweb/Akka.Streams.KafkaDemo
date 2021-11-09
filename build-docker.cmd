@echo off
REM Builds all Docker images

for /R %%f in (*.csproj) do (
    echo "Publishing %%f"
    dotnet publish -c Release %%f
)

echo "Deploying docker compose"
docker compose -f docker-compose.local.yml build