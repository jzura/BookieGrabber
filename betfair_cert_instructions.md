## Step 1: Generate a Private Key
openssl genrsa -out client-2048.key 2048

## Step 2: Generate a Self Signing Request
openssl req -new -key client-2048.key -out client-2048.csr

## Step 3: Generate a Self-Signed Certificate
openssl x509 -req -days 365 \
  -in client-2048.csr \
  -signkey client-2048.key \
  -out client-2048.crt

You now have:
client-2048.key
client-2048.crt
client-2048.csr

upload cert to https://myaccount.betfair.com.au/accountdetails/mysecurity?showAPI=1

cert generated: 4/12/25 (Will expire in a year)