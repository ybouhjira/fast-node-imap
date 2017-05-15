'use strict';

import * as Imap from 'imap';
import * as _ from 'lodash';
import * as Rx from 'rx';
import * as RxNode from 'rx-node';
const {Observable : Obs} = require('rx');


export default class ImapFetcher {

  private _debug;
  private _timeout;
  private _instances;
  private _errors$;
  private _warnings$;
  private debug;


  constructor(debug, timeout, instances) {
    this._debug = debug('fetcher');
    this.debug = debug;
    this._timeout = timeout;
    this._instances = instances;
    this._errors$ = new Rx.Subject();
    this._warnings$ = new Rx.Subject();
  }

  public get errors$() {
    return this._errors$;
  }

  public get warning$() {
    return this._warnings$;
  }

  public get instances() {
    return this._instances;
  }

  private fObserveNewClient(account) {
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

  private fObserveOpenBox(box) {
    return client => Rx.Observable.fromNodeCallback(
        _.bindKey(client, 'openBox')
    )(box, true);
  }

  private makeStartSeqnos(total) {
    const part = Math.ceil(total / this._instances);
    return _.range(1, total, part);
  }

  private getStartSequenceNumbers(account, box) {
    return Rx.Observable
      .create(this.fObserveNewClient(account))
      .do(() => this._debug('Client ready'))
      .flatMap(this.fObserveOpenBox(box))
      .do(box => this._debug('Box open', box.messages.total))
      .map(box =>  box.messages.total)
      .map(total => this.makeStartSeqnos(total))
      .first()
      .retryWhen(errors$ => errors$.do(err => this.debug('error', err.message)));
  }

  public get(account, field, box = 'INBOX') {
    this._debug('ImapFetcher#get()', {account, field, box});
    
    const count$ = new Rx.Subject();
    const data$ = new Rx.Subject();
    const total$ = new Rx.Subject();
    const box$ = new Rx.Subject();

    Obs
        .create(this.fObserveNewClient(account))
        .flatMap(this.fObserveOpenBox(box))
        .timeout(this._timeout)
        .retryWhen(this.retryAndLogTimes(100))
        .share()
        .subscribe(box$);

    box$
      .map(box => box.messages.total)
      .subscribe(total$);
      
    const starts$ = box$
        .map(box => this.makeStartSeqnos(box.messages.total))
        .flatMap(starts => Obs.fromArray(starts));

    const indices$ = Obs.range(0, this._instances);
    Obs.zip(starts$, indices$)
        .withLatestFrom(total$)
        .map(_.flatten)
        .take(this._instances)
        .map(([start, index, total]) => 
          this.runClient(start, account, field, box, index, total)
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

  private runClient(start, account, field, box, index, total) {
    this._debug('Start sequence numbers', start); 

    const lastSeqno$ = new Rx.BehaviorSubject(start);
    const messages$ = new Rx.Subject();
    const client$ = new Rx.Subject();
    const box$ = new Rx.Subject();
    const total$ = new Rx.Subject();
    const result$ = new Rx.Subject();
    
    Rx.Observable
      .create(this.fObserveNewClient(account))
      .retryWhen(this.retryAndLogTimes(100))
      .share()
      .subscribe(client$);    

    client$
      .flatMap(client => this.openBox(box, client))
      .retryWhen(this.retryAndLogTimes(100))
      .share()
      .subscribe(box$);
    
    Rx.Observable
      .zip(box$, client$, Obs.return(index))
      .withLatestFrom(lastSeqno$)
      .map(_.flatten)
      .map(this.calculateInterval.bind(this))
      .flatMap(([client, start, end]) => 
        this.fetchMessages(client, start, end, index, field, box)
      )
      .map(x => _.merge(x, {index}))
      .retryWhen(this.retryAndLogTimes(100, index))
      .share()
      .subscribe(messages$);
    
    messages$
      .withLatestFrom(total$)
      .map(([msg, total]) => _.merge(msg, {total, user: account.user}))
      .take(this.calcEnd(start, total, this.calcPartLength(total)) - start + 1)
      .subscribe(result$);

    box$
      .map(box => box.messages.total)
      .first()
      .subscribe(total$);

    messages$.subscribe(lastSeqno$);

    return result$;
  }

  private calcEnd(start: number, total: number, partLength: number) {
    return start + partLength > total ? total : start + partLength;
  }
  
  private calcPartLength(total) : number{
    return Math.floor(total / this._instances);
  }

  calculateInterval([box, client, index, lastSeqno]) {
    const {messages:{total}} = box;
    const partLength = this.calcPartLength(total);
    const start = lastSeqno;
    const end = this.calcEnd(start, total, partLength);

    this._debug('calculate', [index, start, end, partLength]);
    
    return [client, start, end, index];
  }

  private connect(account) {
    this.debug('fetcher:fn')('#connect()');
    const options = _.merge(account, {tls: true});

    return Rx.Observable
      .range(1, this._instances)
      .do(x => this._debug('Client index', x))
      .flatMap(() => Rx.Observable.create(this.fObserveNewClient(options)));
  }

  private _printWarning(err, index = null) {
    if (index)
      this._debug(`Error (index: ${index})`, err.message);
    else 
      this._debug(`Error: `, err.message);
    this.streamWarning(err);
  }

  private streamError(error) {
    this._errors$.onNext(error);
  }

  private streamWarning(warning) {
    this._warnings$.onNext(warning);
  }

  private openBox(box, client) {
    this.debug('fetcher:fn')('#openBox()');

    return Rx.Observable
      .fromNodeCallback(_.bindKey(client, 'openBox'))(box, true)
      .do(box => this._debug('Mailbox open', box.messages.total))
      .filter(box => box.messages.total != 0);
  }

  private fetchMessages(client, start, end, index, field, box) {
      this.debug('fn')('#fetchMessages()');

      const interval = `${start}:${end}`;
      const bodies = field;
      this.debug('fetcher:interval')('Fetching', {interval, bodies, box});
      const fetch = client.seq.fetch(interval, {bodies});

      return Rx.Observable.create(
        this._observeFetch(fetch, client, {start, end, index})
      );
  }

  private _observeFetch(fetch, client, opts) {
    this.debug('fetcher:fn')('#_observeFetch()');
    let lastFetchedSeqno;

    return observer => {
      fetch.on('error', err => {
        this.debug('fetcher:fn')('#fetch.on(error)');
        observer.onError(err);
        this.streamError(err);
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

  private _createMessageObservable(msg) {
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

  private retryAndLogTimes(times, index = null) {
    return errors$ => {
      errors$.subscribe(this._errors$);

      return errors$
        .do(err => this._printWarning(err, index))
        .take(times);
    };
  }
}



