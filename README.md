# tap-stripe-v2

**tap-stripe-v2** is a Singer Tap capable of syncing data from Stripe. 
**tap-stripe-v2** can be run on [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

This tap is built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-stripe-v2
```

## Configuration

| Name                              | Required | Description                                                                                                                                | Example                                          |
| --------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| `client_secret`                   | Yes\*    | Stripe API secret key used as a bearer token. Either `client_secret` or `access_token` must be provided.                                   | `<STRIPE_SECRET_KEY>`                            |
| `access_token`                    | No\*     | Stripe OAuth access token. Used as a bearer token in place of `client_secret` (e.g. for Stripe Connect).                                   | `<STRIPE_ACCESS_TOKEN>`                          |
| `account_id`                      | No       | Stripe connected account ID. When set, requests are made on behalf of that account via the `Stripe-Account` header.                        | `acct_xxxxxxxxxxxxxxxx`                          |
| `start_date`                      | No       | The earliest record date to sync. Defaults to `2000-01-01T00:00:00.000Z`.                                                                  | `2024-01-01T00:00:00Z`                           |
| `inc_sync_ignore_invoice_status`  | No       | Comma-separated list of invoice statuses to skip during incremental syncs. Defaults to `deleted`.                                          | `deleted,draft`                                  |
| `incremental_balance_transactions`| No       | When `true`, the `balance_transactions` stream syncs incrementally using `created` as the replication key. Defaults to full-table sync.    | `true`                                           |

\* Exactly one of `client_secret` or `access_token` is required.

### Notes

- `client_secret` and `access_token` are functionally equivalent — both are sent as the bearer token in the `Authorization` header. Use whichever matches how you obtained the credential (direct API key vs. Stripe Connect OAuth).
- Setting `account_id` is only relevant when authenticating against the Stripe platform account and syncing data for a specific connected account.
- Setting `incremental_balance_transactions` to `true` is recommended for large Stripe accounts where syncing the full `balance_transactions` history on every run is too slow.

### Example config (minimum)

```json
{
  "client_secret": "<STRIPE_SECRET_KEY>",
  "start_date": "2024-01-01T00:00:00Z"
}
```

### Example config (Stripe Connect with all options)

```json
{
  "access_token": "<STRIPE_ACCESS_TOKEN>",
  "account_id": "acct_xxxxxxxxxxxxxxxx",
  "start_date": "2024-01-01T00:00:00Z",
  "inc_sync_ignore_invoice_status": "deleted,draft",
  "incremental_balance_transactions": true
}
```

A full list of supported settings and capabilities is also available via:

```bash
tap-stripe-v2 --about
```

## Usage

You can easily run `tap-stripe-v2` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-stripe-v2 --version
tap-stripe-v2 --help
tap-stripe-v2 --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_stripe/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-stripe-v2` CLI interface directly using `poetry run`:

```bash
poetry run tap-stripe-v2 --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-stripe-v2
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-stripe-v2 --version
# OR run a test `elt` pipeline:
meltano elt tap-stripe-v2 target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
