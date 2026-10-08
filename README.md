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

## Authentication & User Management V1

DashboardFYEPE now has three roles:
- `superadmin`: full dashboard, Connect Account, posting/bulk/schedule, user management, assign accounts to Admin Medsos.
- `admin_medsos`: sees and manages accounts assigned to that admin; can update account metadata and statistics/posting functions.
- `customer`: read-only customer dashboard at `/customer`, with aggregate Views/Likes/Comments and account performance. Customer cannot access posting or account-management routes.

### First Super Admin on Render
Add these Environment Variables once:
- `INITIAL_ADMIN_USERNAME` = username Super Admin
- `INITIAL_ADMIN_PASSWORD` = strong password (minimum 8 chars recommended)
- `INITIAL_ADMIN_NAME` = display name
- `INITIAL_ADMIN_EMAIL` = email

After the first deployment, login at `/login`. The bootstrap user is created only when the `users` table is empty. Change/disable users from **User Management**.

### Important
Existing TikTok `accounts` rows, access tokens, refresh tokens, statistics and connected accounts are preserved. The migration only adds `users`, `user_keywords`, and `accounts.assigned_admin_id`.
