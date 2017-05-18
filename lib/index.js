'use strict';

const  Imap = require('imap');
const _ = require('lodash');
const Rx = require('rx');
const RxNode = require('rx-node');
const {Observable : Obs} = require('rx');


export default class ImapFetcher {

  constructor(debug, timeout, instances) {
    this._debug = debug('rx-imap');
    this.debug = debug;
    this._timeout = timeout;
    this._instances = instances;
    this._errors$ = new Rx.Subject();
    this._warnings$ = new Rx.Subject();
  }

  get errors$() {
    return this._errors$;
  }

  get warning$() {
    return this._warnings$;
  }

  get instances() {
    return this._instances;
  }

  _fObserveNewClient(account) {
    return o => {
      const imap = new Imap(_.merge(account, {tls: true}));
        
      imap.once('ready', () => o.onNext(imap));
      imap.on('error', err => {
        o.onError(err);
        imap.end();
      });
      imap.connect();

      return () => imap.end();
    }
  }

  _fObserveOpenBox(box) {
    return client => Rx.Observable.fromNodeCallback(
        _.bindKey(client, 'openBox')
    )(box, true);
  }

  _makeStartSeqnos(total) {
    const part = Math.ceil(total / this._instances);
    return _.range(1, total, part);
  }

  _getStartSequenceNumbers(account, box) {
    return Rx.Observable
      .create(this._fObserveNewClient(account))
      .do(() => this._debug('Client ready'))
      .flatMap(this._fObserveOpenBox(box))
      .do(box => this._debug('Box open', box.messages.total))
      .map(box =>  box.messages.total)
      .map(total => this._makeStartSeqnos(total))
      .first()
      .retryWhen(errors$ => errors$.do(err => this.debug('error', err.message)));
  }

   get(account, field, box = 'INBOX') {
    this._debug('ImapFetcher#get()', {account, field, box});
    
    const count$ = new Rx.Subject();
    const data$ = new Rx.Subject();
    const total$ = new Rx.Subject();
    const box$ = new Rx.Subject();

    Obs
        .create(this._fObserveNewClient(account))
        .flatMap(this._fObserveOpenBox(box))
        .timeout(this._timeout)
        .retryWhen(this._retryAndLogTimes(100))
        .share()
        .subscribe(box$);

    box$
      .map(box => box.messages.total)
      .subscribe(total$);
      
    const starts$ = box$
        .map(box => this._makeStartSeqnos(box.messages.total))
        .flatMap(starts => Obs.fromArray(starts));

    const indices$ = Obs.range(0, this._instances);
    Obs.zip(starts$, indices$)
        .withLatestFrom(total$)
        .map(_.flatten)
        .take(this._instances)
        .map(([start, index, total]) => 
          this._runClient(start, account, field, box, index, total)
        )
        .reduce((array, o$) => array.concat([o$]), [])
        .flatMap(obs$ => Obs.merge(obs$))
        .subscribe(data$);

    data$
      .scan(count => count + 1, 0)
      .subscribe(count$);

    return data$
      .withLatestFrom(count$)
      .map(([msg, count]) => _.merge(msg, {count}));
  }

  _runClient(start, account, field, box, index, total) {
    this._debug('Start sequence numbers', start); 

    const lastSeqno$ = new Rx.BehaviorSubject(start);
    const messages$ = new Rx.Subject();
    const client$ = new Rx.Subject();
    const box$ = new Rx.Subject();
    const total$ = new Rx.Subject();
    const result$ = new Rx.Subject();
    
    Rx.Observable
      .create(this._fObserveNewClient(account))
      .retryWhen(this._retryAndLogTimes(100))
      .share()
      .subscribe(client$);    

    client$
      .flatMap(client => this._openBox(box, client))
      .retryWhen(this._retryAndLogTimes(100))
      .share()
      .subscribe(box$);
    
    Rx.Observable
      .zip(box$, client$, Obs.return(index))
      .withLatestFrom(lastSeqno$)
      .map(_.flatten)
      .map(this._calculateInterval.bind(this))
      .flatMap(([client, start, end]) => 
        this._fetchMessages(client, start, end, index, field, box)
      )
      .map(x => _.merge(x, {index}))
      .retryWhen(this._retryAndLogTimes(100, index))
      .share()
      .subscribe(messages$);
    
    messages$
      .withLatestFrom(total$)
      .map(([msg, total]) => _.merge(msg, {total, user: account.user}))
      .take(this._calcEnd(start, total, this._calcPartLength(total)) - start + 1)
      .subscribe(result$);

    box$
      .map(box => box.messages.total)
      .first()
      .subscribe(total$);

    messages$.subscribe(lastSeqno$);

    return result$;
  }

  _calcEnd(start, total, partLength) {
    return start + partLength > total ? total : start + partLength;
  }
  
  _calcPartLength(total){
    return Math.floor(total / this._instances);
  }

  _calculateInterval([box, client, index, lastSeqno]) {
    const {messages:{total}} = box;
    const partLength = this._calcPartLength(total);
    const start = lastSeqno;
    const end = this._calcEnd(start, total, partLength);

    this._debug('calculate', [index, start, end, partLength]);
    
    return [client, start, end, index];
  }

  _connect(account) {
    this.debug('fetcher:fn')('#connect()');
    const options = _.merge(account, {tls: true});

    return Rx.Observable
      .range(1, this._instances)
      .do(x => this._debug('Client index', x))
      .flatMap(() => Rx.Observable.create(this._fObserveNewClient(options)));
  }

  _printWarning(err, index = null) {
    if (index)
      this._debug(`Error (index: ${index})`, err.message);
    else 
      this._debug(`Error: `, err.message);
    this._streamWarning(err);
  }

  _streamError(error) {
    this._errors$.onNext(error);
  }

  _streamWarning(warning) {
    this._warnings$.onNext(warning);
  }

  _openBox(box, client) {
    this.debug('fetcher:fn')('#openBox()');

    return Rx.Observable
      .fromNodeCallback(_.bindKey(client, 'openBox'))(box, true)
      .do(box => this._debug('Mailbox open', box.messages.total))
      .filter(box => box.messages.total != 0);
  }

  _fetchMessages(client, start, end, index, field, box) {
      this.debug('fn')('#fetchMessages()');

      const interval = `${start}:${end}`;
      const bodies = field;
      this.debug('fetcher:interval')('Fetching', {interval, bodies, box});
      const fetch = client.seq.fetch(interval, {bodies});

      return Rx.Observable.create(
        this._observeFetch(fetch, client, {start, end, index})
      );
  }

  _observeFetch(fetch, client, opts) {
    this.debug('fetcher:fn')('#_observeFetch()');
    let lastFetchedSeqno;

    return observer => {
      fetch.on('error', err => {
        this.debug('fetcher:fn')('#fetch.on(error)');
        observer.onError(err);
        this._streamError(err);
      });

      fetch.once('end', () => {
        this.debug('fetcher:fn')('#fetch.on(end)');
        if (lastFetchedSeqno == opts.end) {
          this._debug('Done fetching all messages!', _.merge(opts, {lastFetchedSeqno}));
          observer.onCompleted();
        } else {
          observer.onError({
            message: `End event without actually finishing ` + 
              `(last: ${lastFetchedSeqno}, start: ${opts.start}, end: ${opts.end})`
          });
        }
        client.end();
      });

      fetch.on('message', (msgStream, seqno) => {
        this.debug('fetcher:fn')('#fetch.on(message)');
        this
          ._createMessageObservable(msgStream)
          .subscribe(
            msg => {
              observer.onNext({msg, seqno});
            }, 
            err => { },
            () => {
              lastFetchedSeqno = seqno;
              this.debug('lastseqno')('update last seqno', _.merge(opts, {lastFetchedSeqno}));
            }
          );
      });
    };
  }

  _createMessageObservable(msg) {
    this.debug('fetcher:fn')('#_createMessageRx.Observable()');
    
    return Obs.create(msgObserver => {
      msg.on('body', stream => {
        this.debug('fetcher:fn')('#msg.on(body)');
        RxNode
          .fromStream(stream)
          .map(String)
          .reduce((x, y) => x + y, '')
          .subscribe(msg => {
            msgObserver.onNext(msg);
          });
      });

      msg.once('end', () => {
        this.debug('fetcher:fn')('#msg.on(end)');
        msgObserver.onCompleted();
      });
    });
  }

  _retryAndLogTimes(times, index = null) {
    return errors$ => {
      errors$.subscribe(this._errors$);

      return errors$
        .do(err => this._printWarning(err, index))
        .take(times);
    };
  }
}



