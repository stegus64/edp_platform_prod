# Order Updates Function App

Azure Functions (Python) that process order-related events (Event Hubs, timer, and storage).

## Overview

This Function App receives and processes order update events from Event Hubs.

## Prerequisites

- Python 3.8+ (recommend 3.8–3.11 for Azure Functions Python support).
- Azure Functions Core Tools (recommended v4+).
- Azurite (for local Blob/Queue/Table emulation) if you want to emulate storage locally.
- Git and a code editor (VS Code recommended).
 - Azure CLI (`az`) — required for some deployment workflows and useful for configuration management.

## Setup (local development)

1. Prepare environment

   ```powershell
   cd function
   az login
   ```
   Ensure required environment variables and connection strings are present in `local.settings.json`. Typical keys to check:

    If you do not have an `local.settings.json`, copy a template or create one and add the above keys. Do not commit secrets to source control.

3. Run the Functions host locally. The project is configured to launch from VS Code — simply `cd function` and press F5. The debug/launch settings will start the Functions host and attach the debugger.

   Alternatively you can run the host directly:

   ```powershell
   cd function
   func host start
   ```

Or use the VS Code task `func: host start` which depends on the `pip install (functions)` task.

## Functions (high-level)

- `orderupdates_eh_trigger` — Handles generic order update events from Event Hubs.
- `timer_trigger` — Runs scheduled maintenance or polling tasks.

Each function is implemented in its own folder and exposes an Azure Functions entry point. Shared code (schemas, helpers) live under `shared/` for reuse.

## Deployment

```powershell
# from the `function` folder
func azure functionapp publish func-edp-prod-01 --build remote
```

Ensure production connection strings and configuration are set in the Function App's Application Settings in Azure (do not use `local.settings.json` in production). You may also use `az` commands to manage app settings and deployments if needed.
