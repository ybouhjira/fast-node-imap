# rx-imap
Just a node-imap wrapper 
## Why use this
- Fetch messages from very large mailboxes.
- Restarts on error.
- Fetches in parallel.

## Install
```
npm i rx-imap -S
```

## How to use
```js
const RxImap = require('rx-imap');

const imap = new RxImap({
    instances: 32,
    timeout: 3000,
});

const account = {
    user: 'seomeone@host.com'
    host: 'imap.host.com',
    password: 'password@123',
    port: 993
};

const field = 'HEADER.FIELDS (REPLY-TO)';

imap.get(account, field, 'INBOX')
    .subscribe(x => console.log(x));
```
