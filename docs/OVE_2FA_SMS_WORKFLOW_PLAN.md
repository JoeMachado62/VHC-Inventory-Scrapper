# OVE 2FA SMS Workflow Plan

## Purpose

Add an operator-assisted 2FA workflow for OVE/Manheim authentication without blocking the Lux + Playwright hybrid scraper rollout.

The core hybrid model should remain:

- Lux/Open AGI handles messy login/authentication handoff when needed.
- Playwright/CDP handles deterministic scraper work: saved-search exports, hot-deal scraping, detail/condition-report requests, and VPS pushes.
- Tab compaction and session checks keep each Chrome profile clean before and after browser work.

The 2FA SMS workflow is a follow-on reliability layer. It should not be required before promoting the proven hybrid changes.

## Current Findings

- OpenClaw gateway is running and healthy for Telegram.
- OpenClaw does not currently expose a native Telnyx/SMS channel through `openclaw message send`.
- Telnyx API credentials are available in `/home/joema/.env`.
- Telnyx REST API access works.
- The configured sender number `+1-239-216-9696` is active and assigned to Telnyx messaging profile `Ezwai`.
- The number supports domestic SMS/MMS routing.
- Test SMS API submission was accepted by Telnyx but failed carrier delivery.
- Telnyx message record showed error `40010`: sending number is not 10DLC registered.
- API audit showed no current 10DLC brand records and no phone-number campaign assignment for the sender number.

Important distinction: Telnyx “Domestic Inbound & Outbound SMS” enablement means the number can be used for messaging on Telnyx. It does not necessarily mean the number is registered with an approved US A2P 10DLC campaign, which carriers require for outbound long-code delivery.

## Desired Operator Flow

1. Scraper starts a browser operation for Login A or Login B.
2. Session preflight checks whether OVE is authenticated.
3. If OVE/Manheim asks for a 2FA code, scraper creates a short-lived 2FA challenge.
4. Notification service sends an operator message:
   - Login A operator: `+12398881606`
   - Login B operator: `+17867318493`
5. Operator receives the real code from OVE/Manheim separately.
6. Operator replies to the notification with the code.
7. The automation validates the reply belongs to the active challenge.
8. Lux or Playwright enters the code into the visible 2FA field.
9. Automation submits the form and chooses “trust this device” if prompted.
10. Challenge state is cleared after success, timeout, or manual cancellation.

## Proposed Architecture

### Components

- `mfa_bridge` module
  - Owns challenge creation, lookup, timeout, and completion.
  - Stores active challenge state in `artifacts/_state/ove_mfa_challenges.json`.
  - Does not log raw 2FA codes.

- Telnyx/OpenClaw notification adapter
  - Initially can call Telnyx REST API directly.
  - Later can be wrapped as an OpenClaw skill/plugin once the native channel story is clear.

- Browser 2FA detector
  - Reuses existing page text signals: `verification code`, `security code`, `two-factor`, `multi-factor`, `authenticator`, `text message`.
  - Detects the code input and submit button.

- Browser 2FA submitter
  - Enters the operator-provided code.
  - Submits once.
  - Handles trust-device prompt if visible.
  - Never loops code submission repeatedly.

### Challenge State

Each active challenge should include:

- `challenge_id`
- `login_track`: `login_a` or `login_b`
- `chrome_port`: `9222` or `9223`
- `operator_phone`
- `created_at_utc`
- `expires_at_utc`
- `status`: `pending`, `completed`, `expired`, `failed`, `cancelled`
- `browser_context`: current URL/title, no secrets
- `attempt_count`

Raw codes should only exist in memory long enough to submit the form. If persistence is unavoidable, store only encrypted/short-TTL data and avoid writing the code into normal logs.

## Telnyx / 10DLC Go-Live Requirements

Before SMS can be used reliably for the 2FA workflow, the sender number must be approved for US long-code outbound messaging.

### Brand Registration

Register a 10DLC Brand for Virtual CarHub.

Required information:

- Legal company name exactly as IRS records it.
- EIN / Federal Tax ID.
- Business address.
- Business phone.
- Business contact email.
- Website: `https://virtualcarhub.com`
- Authorized representative name.
- Authorized representative title.
- Authorized representative email.
- Authorized representative phone.

### Campaign Registration

Register a 10DLC Campaign that accurately matches the SMS use case.

Required information:

- Use case selection.
- Campaign description.
- At least two sample SMS messages.
- Message flow / opt-in explanation.
- HELP response.
- STOP / opt-out response.
- Opt-in keywords.
- Opt-out keywords.
- Help keywords.
- Whether messages include links.
- Whether messages include phone numbers.

The use case must match the actual traffic. This workflow is not sending the OVE 2FA code itself; it is notifying an internal operator that a browser session requires a code. The campaign should be described as internal operational authentication assistance/account notifications, unless Telnyx guidance says another use case is more appropriate.

### Number Assignment

After brand and campaign approval:

- Assign `+1-239-216-9696` to the approved 10DLC campaign.
- Verify the Telnyx phone-number record shows a non-null `messaging_campaign_id`.
- Send a test SMS.
- Query message status and confirm delivery, not merely API acceptance.

### Compliance Notes

- Telnyx/carriers may charge registration, campaign review, or resubmission fees.
- Registration details must be exact and consistent.
- Sample messages, campaign description, opt-in flow, website, and actual messages must align.
- If the campaign is rejected, update the registration rather than forcing messages through unregistered long-code paths.

## Example SMS Content

Initial prompt:

```text
Virtual CarHub auth alert: OVE Login B needs a verification code. Reply with the 6-digit code. Reply STOP to opt out.
```

Success acknowledgement:

```text
Virtual CarHub auth alert: Code received for OVE Login B. The scraper is completing login now.
```

Timeout:

```text
Virtual CarHub auth alert: OVE Login B verification timed out. Start a new login attempt if needed.
```

HELP:

```text
Virtual CarHub auth alerts help operators complete OVE login verification for scraper browser profiles. Reply STOP to opt out.
```

STOP:

```text
You have opted out of Virtual CarHub auth alerts. Reply START to opt back in.
```

These are drafts and should be reviewed against the final Telnyx campaign use case before submission.

## Implementation Phases

### Phase 1: Keep Hybrid Rollout Moving

- Promote Lux + Playwright hybrid auth handoff.
- Promote tab compaction.
- Promote Login B stale OVE cookie recovery workflow.
- Promote export-button ordering fix.
- Restart production scraper after promotion and smoke tests.

This phase should not wait for Telnyx/10DLC.

### Phase 2: Telnyx Readiness

- Confirm whether Telnyx CLI is installed anywhere outside current PATH.
- If useful, install or expose Telnyx CLI for operator convenience.
- Complete 10DLC brand registration.
- Complete campaign registration.
- Assign `+1-239-216-9696` to approved campaign.
- Confirm real SMS delivery with message-status lookup.

### Phase 3: Minimal 2FA Bridge

- Add `mfa_bridge` module.
- Add direct Telnyx send/receive integration or OpenClaw wrapper.
- Add challenge state file.
- Add test mode for simulated inbound replies.
- Add CLI:
  - create challenge
  - send prompt
  - submit code
  - expire challenge
  - inspect status without showing code

### Phase 4: Browser Integration

- Detect 2FA in `lux_auth_handoff.py`.
- Create challenge and notify operator.
- Wait for reply with a bounded timeout.
- Enter code into browser.
- Click submit.
- Trust device when offered.
- Hand back to Playwright.

### Phase 5: OpenClaw Skill

Create an OpenClaw skill for:

- Sending 2FA prompts.
- Receiving/validating operator replies.
- Reporting challenge status.
- Future ad hoc operator workflows:
  - condition report requests
  - vehicle search inquiries
  - scraper status checks
  - manual sync/hot-deal triggers

## Test Plan

### Telnyx

- Send SMS to Login A operator.
- Confirm delivery on handset.
- Query Telnyx message record and verify delivered/completed state.
- Send inbound reply and confirm webhook/receiver captures it.

### Browser

- Simulate challenge state without real OVE 2FA.
- Verify code entry selectors on a captured Manheim 2FA page.
- Verify timeout path does not retry or spam.
- Verify wrong-phone replies are rejected.
- Verify stale challenge replies are rejected.

### End-to-End

- Force Login A to a 2FA state in a controlled test window.
- Confirm operator receives prompt.
- Reply with code.
- Confirm browser completes login.
- Confirm Playwright can immediately run a detail poll or manual VIN scrape.
- Repeat for Login B and a saved-search sync preflight.

## Rollback Plan

- Disable 2FA bridge with env flag, for example `OVE_MFA_BRIDGE_ENABLED=false`.
- Keep Lux + Playwright hybrid auth active.
- Fall back to existing manual login scripts:
  - `scripts/manual_login_a.ps1`
  - `scripts/manual_login_b.ps1`
- Leave production scraper able to park safely on auth lockout instead of retrying aggressively.

## Open Questions

- Is there already a Telnyx 10DLC campaign in another Telnyx account or subaccount?
- Should Login A and Login B use the same sender number or separate Telnyx numbers?
- Should the first production 2FA notification path use SMS, Telegram, or both?
- Should inbound SMS replies be received by Telnyx webhooks directly or routed through OpenClaw?
- Who is the official authorized representative for 10DLC registration?

## Recommendation

Proceed with the hybrid Lux + Playwright production promotion first.

Treat the 2FA SMS workflow as a separate project with its own Telnyx compliance milestone. Once Telnyx 10DLC delivery is confirmed, add the 2FA bridge as an optional recovery layer rather than as a required dependency for the scraper’s normal operation.
