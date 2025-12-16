# BookieGrabber

BookieGrabber is a data-collection and post-processing pipeline that continuously fetches odds from multiple bookmakers and Betfair, normalises the data, applies merge rules, and exports ready-to-bet spreadsheets multiple times per day. It forms the backend of an automated betting decision system.

---

## 🆕 Adding a New League

To onboard a new league, you must manually build merge-key mappings so team names align between bookmakers and Betfair.

1. Pull raw Betfair event data for the new league using the grabber.
2. Inspect the `events` column to determine Betfair’s team naming.
3. Manually map each bookmaker team name → Betfair team name.
4. Save the mapping file as: mappings/<league-slug>/team_name_map.json

This ensures BookieGrabber can correctly merge odds and events across all bookies.

---

## 🔐 Betfair Certificate Renewal

Betfair API access requires a client certificate which expires every 12 months.  
Follow the steps below to generate and renew your certificate.

### Step 1 — Generate Private Key
```bash
openssl genrsa -out client-2048.key 2048
```
### Step 2: Generate a Self Signing Request
```bash
openssl req -new -key client-2048.key -out client-2048.csr
```
### Step 3: Generate a Self-Signed Certificate
```bash
openssl x509 -req -days 365 \
  -in client-2048.csr \
  -signkey client-2048.key \
  -out client-2048.crt
```
    You now have:
    client-2048.key
    client-2048.crt
    client-2048.csr

Upload the .crt file at: 

https://myaccount.betfair.com.au/accountdetails/mysecurity?showAPI=1

Last generated: 4/12/25

Expires: 12 months from creation.