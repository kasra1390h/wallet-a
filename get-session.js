// Run this ONCE on your own computer (not on Render/Railway) to generate TG_SESSION.
// 1) npm install
// 2) Get api_id and api_hash from https://my.telegram.org -> API development tools
// 3) node get-session.js
// 4) Log in with YOUR OWN Telegram account (phone number + code Telegram sends you)
// 5) Copy the printed session string into the TG_SESSION env var on your host.
// This does NOT touch your bot token. It uses your personal account only to read
// Telegram's public client config (stars_usd_sell_rate_x1000) — nothing else.
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const input = require('input');

(async () => {
  const apiId = +(await input.text('api_id (from my.telegram.org): '));
  const apiHash = await input.text('api_hash (from my.telegram.org): ');
  const client = new TelegramClient(new StringSession(''), apiId, apiHash, { connectionRetries: 5 });
  await client.start({
    phoneNumber: async () => await input.text('Phone number (with country code, e.g. +98...): '),
    password: async () => await input.text('2FA password (leave empty if none): '),
    phoneCode: async () => await input.text('Code Telegram just sent you: '),
    onError: (err) => console.error(err),
  });
  console.log('\nSave these in your host environment variables:\n');
  console.log('TG_API_ID=' + apiId);
  console.log('TG_API_HASH=' + apiHash);
  console.log('TG_SESSION=' + client.session.save());
  await client.disconnect();
  process.exit(0);
})();
