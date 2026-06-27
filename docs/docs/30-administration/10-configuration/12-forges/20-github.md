---
toc_max_heading_level: 2
---

# GitHub

Woodpecker comes with built-in support for GitHub and GitHub Enterprise.
To use Woodpecker with GitHub the following environment variables should be set for the server component:

```ini
WOODPECKER_GITHUB=true
WOODPECKER_GITHUB_CLIENT=YOUR_GITHUB_CLIENT_ID
WOODPECKER_GITHUB_SECRET=YOUR_GITHUB_CLIENT_SECRET
```

You will get these values from GitHub when you register your OAuth application.
To do so, go to Settings -> Developer Settings -> GitHub Apps -> New Oauth2 App.

::::warning
A GitHub App cannot replace the OAuth2 app for user login, because user access tokens are not refreshed automatically. A GitHub App can, however, be configured _in addition_ to the OAuth2 app to report workflow status via the Checks API — see [GitHub App for the Checks API](#github-app-for-the-checks-api-optional).
::::

## App Settings

- Name: An arbitrary name for your App
- Homepage URL: The URL of your Woodpecker instance
- Callback URL: `https://<your-woodpecker-instance>/authorize`
- (optional) Upload the Woodpecker Logo: <https://avatars.githubusercontent.com/u/84780935?s=200&v=4>

## Client Secret Creation

After your App has been created, you can generate a client secret.
Use this one for the `WOODPECKER_GITHUB_SECRET` environment variable.

## GitHub App for the Checks API (optional)

By default Woodpecker reports each workflow's status through GitHub's commit-status API, which only supports the `pending`, `success`, `failure` and `error` states. To instead report workflows as **check-runs** — which also support `skipped` and `neutral` conclusions and are grouped under a check-suite — configure a GitHub App in addition to the OAuth2 app above.

The OAuth2 app is still required for user login; the GitHub App is used only to report status via the [Checks API](https://docs.github.com/en/rest/checks), which is not available to OAuth tokens.

1. Create a GitHub App (Settings -> Developer Settings -> GitHub Apps -> New GitHub App) granting **read & write** access to **Checks**, and generate a private key.
2. Install the App on the organizations or repositories Woodpecker builds.
3. Set the following server environment variables:

```ini
WOODPECKER_GITHUB_APP_ID=YOUR_APP_ID
WOODPECKER_GITHUB_APP_PRIVATE_KEY_FILE=/path/to/private-key.pem
```

When configured, workflows are reported as check-runs; otherwise Woodpecker keeps using the commit-status API. With the Checks API you can also enable [`WOODPECKER_REPORT_SKIPPED_WORKFLOWS`](../10-server.md#report_skipped_workflows) to surface workflows filtered out by their `when` conditions as grey `skipped` checks.

## Configuration

This is a full list of configuration options. Please note that many of these options use default configuration values that should work for the majority of installations.

---

### GITHUB

- Name: `WOODPECKER_GITHUB`
- Default: `false`

Enables the GitHub driver.

---

### GITHUB_URL

- Name: `WOODPECKER_GITHUB_URL`
- Default: `https://github.com`

Configures the GitHub server address.

---

### GITHUB_CLIENT

- Name: `WOODPECKER_GITHUB_CLIENT`
- Default: none

Configures the GitHub OAuth client id to authorize access.

---

### GITHUB_CLIENT_FILE

- Name: `WOODPECKER_GITHUB_CLIENT_FILE`
- Default: none

Read the value for `WOODPECKER_GITHUB_CLIENT` from the specified filepath.

---

### GITHUB_SECRET

- Name: `WOODPECKER_GITHUB_SECRET`
- Default: none

Configures the GitHub OAuth client secret. This is used to authorize access.

---

### GITHUB_SECRET_FILE

- Name: `WOODPECKER_GITHUB_SECRET_FILE`
- Default: none

Read the value for `WOODPECKER_GITHUB_SECRET` from the specified filepath.

---

### GITHUB_MERGE_REF

- Name: `WOODPECKER_GITHUB_MERGE_REF`
- Default: `true`

---

### GITHUB_SKIP_VERIFY

- Name: `WOODPECKER_GITHUB_SKIP_VERIFY`
- Default: `false`

Configure if SSL verification should be skipped.

---

### GITHUB_PUBLIC_ONLY

- Name: `WOODPECKER_GITHUB_PUBLIC_ONLY`
- Default: `false`

Configures the GitHub OAuth client to only obtain a token that can manage public repositories.

---

### GITHUB_APP_ID

- Name: `WOODPECKER_GITHUB_APP_ID`
- Default: none

GitHub App id. When set together with a private key, workflow status is reported via the GitHub Checks API (enabling `skipped` / `neutral` conclusions) instead of the commit-status API.

---

### GITHUB_APP_PRIVATE_KEY

- Name: `WOODPECKER_GITHUB_APP_PRIVATE_KEY`
- Default: none

GitHub App private key in PEM format, used to authenticate as the App for the Checks API.

---

### GITHUB_APP_PRIVATE_KEY_FILE

- Name: `WOODPECKER_GITHUB_APP_PRIVATE_KEY_FILE`
- Default: none

Read the value for `WOODPECKER_GITHUB_APP_PRIVATE_KEY` from the specified filepath.
