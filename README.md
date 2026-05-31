# drive-to-r2-mover

Ephemeral Render web service that copies a Google Drive folder to a Cloudflare R2 bucket using rclone.

Visit the service URL to see live status.

Required env vars: `DRIVE_FOLDER_ID`, `GDRIVE_TOKEN_JSON`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`, `R2_BUCKET`.
