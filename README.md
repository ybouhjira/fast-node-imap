## Why use this
- Fetch messages from very large mailboxes.
- Restarts on error.
- Fetches in parallel.

# Install
```
npm i rx-imap -S
```

# How to use
```js
const RxImap = require('rx-imap');

const imap = new RxImap({
    instances: 32,
    timeout: 3000,
});

const account = {
    host: '',
    password: '',
    port: 993
}
const field = 'HEADER.FIELDS (REPLY-TO)';

imap.get(account, field, 'INBOX')
    .subscribe(x => console.log(x));
```