# DashboardFYEPE V1

## Render
Build command: `pip install -r requirements.txt`
Start command: `gunicorn app:app`

Environment variables: copy `.env.example` values and replace Sandbox credentials.
Never commit Client Secret.

Custom domain: `dashboardFYEPE.islammoderat.my.id`
TikTok redirect URI must exactly match:
`https://dashboardFYEPE.islammoderat.my.id/auth/tiktok/callback`

V1 implements TikTok OAuth Login Kit, server-side token exchange, basic user profile display, and Creator Info query. Direct file publishing UI is intentionally disabled until the upload handler is implemented/tested.
