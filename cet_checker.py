#!/usr/bin/env python2.7
#   cet_checker.py - check CET score(s)
#   Copyright (C) 2009,2010,2011  bhuztez <bhuztez@gmail.com>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU Affero General Public License as
#   published by the Free Software Foundation, either version 3 of the
#   License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Affero General Public License for more details.
#
#   You should have received a copy of the GNU Affero General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
check CET score(s)

Check your CET score directly, or speed up collecting CET scores."""

__author__  = 'bhuztez <bhuztez@gmail.com>'
__version__ = '0.03.1012'

import sys, re, pycurl, threading, Queue, sqlite3, bisect, StringIO, os, os.path

try:
    from PIL import Image
except ImportError:
    import Image

bitstrings = [ 
'\xe3\xc1\x9c\x9c\x9c\x9c\x9c\x9c\x9c\x9c\x9c\x9c\xc1\xe3\xff',
'\xf3\xe3\xc3\xd3\xf3\xf3\xf3\xf3\xf3\xf3\xf3\xf3\xc0\xc0\xff',
'\xe3\xc1\x9c\xbc\xfc\xf9\xf3\xe7\xcf\x9f\x9f\x9f\x80\x80\xff',
'\xe3\xc1\x9c\xfc\xf9\xf9\xf3\xe7\xf3\xf9\xf9\xfc\x9c\xc1\xe3',
'\xfd\xf9\xf1\xe1\xc9\x99\x39\x39\x39\x39\x00\x00\xf9\xf9\xf9',
'\x80\x80\x9f\x9f\x9f\x83\x81\xf9\xfc\xfc\x9c\x9c\xc1\xe3\xff',
'\xe3\xc0\xcc\x9f\x9f\x9f\x9f\x83\x81\x9c\x9c\x9c\x80\xe1\xff',
'\x80\x80\x9c\xfc\xfc\xf9\xf9\xf3\xf3\xe7\xe7\xe7\xe7\xe7\xff',
'\xe3\xc1\x9c\x9c\x9c\xc9\xc9\xe7\xc9\xc9\x9c\x9c\x9c\xc1\xe3',
'\xe3\xc1\x9c\x9c\x9c\xc0\xc4\xfc\xfc\xfc\xfc\x9c\x98\xc3\xe7' ]

masks = [ Image.fromstring("1", (8,15), s) for s in bitstrings ]
background = Image.new("1", (8,15), 1)
positions = [ (6+i*11,5,14+i*11,20) for i in range(0,8) ]


def match(img, mask):
    return Image.composite(background, img, mask).tostring() == mask.tostring()


def vcd(img):
    for i, mask in enumerate(masks):
        if match(img, mask):
            return i


def validate(img):
    return "".join([ "%d"%(vcd(img.crop(p).convert("1"))) for p in positions ])


def vc(path):
    return validate(Image.open(path))


def fetch_vc(tid):
    path = "vc/%s.png"%(tid)
    if os.path.exists(path) and os.path.getsize(path):
        return path

    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, "http://va.99sushe.com/validate.html?id=%s"%(tid))
    curl.setopt(pycurl.HTTPHEADER, ["Referer: http://cet.99sushe.com/"])

    buf = StringIO.StringIO()
    curl.setopt(pycurl.WRITEFUNCTION, buf.write)

    curl.perform()

    if (curl.getinfo(pycurl.HTTP_CODE) == 200):
        content = buf.getvalue()
        f = open(path, "w")
        f.write(content)
        f.close()

        if os.path.getsize(path):
            return path


def get_vc(tid):
    path = fetch_vc(tid)
    if path is None:
        return

    return vc(path)


class Collector(Queue.Queue):
    
    def __init__(self, committer, maxsize=0):
        Queue.Queue.__init__(self, maxsize) 
        self.committer = committer
    
    def serve(self, is_finished):
        while True:
            try:
                result = self.get(block=False)
                self.committer.commit(result)
                self.task_done()
            except Queue.Empty:
                if is_finished(): break
                self.not_empty.acquire()
                self.not_empty.wait(1)
                self.not_empty.release()
        
        self.committer.finish()


def task(func):

    def wrapper(*args, **kwargs):
        return (func, args, kwargs)     
    return wrapper


class Worker(object):
    
    def work(self, task):
        (func, args, kwargs) = task
        return func(*args, **kwargs)
    
    def start(self, distributer, collector):
        thread = threading.Thread(target=self.run, args=(distributer, collector))
        thread.setDaemon(True)
        thread.start()
    
    def run(self, distributer, collector):
        try:
            while True:
                task = distributer.get(block=False)
                try:
                    results, tasks  = self.work(task)
                    for task in tasks: distributer.put(task)
                    for result in results: collector.put(result)
                finally:
                    distributer.task_done()
        except Queue.Empty:
            distributer.worker_quit()


class TaskQueue(Queue.Queue):

    def __init__(self, collector, worker_factory=Worker, max_workers=1, maxsize=0):
        Queue.Queue.__init__(self, maxsize)
        self.max_workers = max_workers
        self.worker_factory = worker_factory
        self.collector = collector
        self.workers = 0
        self.workers_changed = threading.Condition(self.mutex)

    def put(self, item, block = True, timeout = None):
        Queue.Queue.put(self, item, block, timeout)
        self.workers_changed.acquire()
        if self.workers < self.max_workers:
            self.add_worker()
            self.workers += 1        
        self.workers_changed.release()

    def add_worker(self):
        worker = self.worker_factory()
        worker.start(self, self.collector)
    
    def is_finished(self):
        self.all_tasks_done.acquire()
        finished = not self.unfinished_tasks
        self.all_tasks_done.release()
        return finished

    def join(self):
        self.collector.serve(self.is_finished)
        Queue.Queue.join(self)
    
    def worker_quit(self):
        self.workers_changed.acquire()
        self.workers -= 1        
        self.workers_changed.release()
    

class Committer(object):

    def __init__(self, **kwargs):
        self.fields = kwargs.keys()
        self.format = '	|'.join(["%%(%s)s"%(key) for key in kwargs ])
        print '	|'.join(["%s"] * len(self.fields))%tuple(self.fields)
   
    def commit(self, result):
        print self.format%result
    
    def finish(self):
        pass
    
    def dump(self):
        return []


class SqliteCommitter(Committer):

    def __init__(self, _database, _table, **kwargs):
        super(SqliteCommitter, self).__init__(**kwargs)
        self.con = sqlite3.connect(_database)
        fields = ','.join(['%s %s'%(key, kwargs[key]) for key in kwargs])
        self.con.execute('create table if not exists %s (%s)'%(_table, fields))
        self.con.row_factory = sqlite3.Row
        
        self.table = _table
    
    @property
    def insert_sql(self):
        if not hasattr(self, '_insert_sql'):
            self._insert_sql = 'insert or ignore into %s (%s) values (%s)'%(
                self.table,
                ','.join(self.fields),
                ','.join(['?']*len(self.fields)) )
        
        return self._insert_sql
    
    def values(self, **kwargs):
        return tuple( kwargs[key] for key in self.fields )
    
    def commit(self, result):
        self.con.execute(self.insert_sql, self.values(**result))
    
    def finish(self):
        self.con.commit()
    
    def dump(self):
        sql = 'select %s from %s'%(','.join(self.fields), self.table)
        for row in self.con.execute(sql):
            yield self.format%dict(((key,row[n]) for n,key in enumerate(self.fields)))


class CachedCommitter(SqliteCommitter):
    
    def __init__(self, _cache_size, _database, _table, **kwargs):
        super(CachedCommitter, self).__init__(_database, _table, **kwargs)

        self.cache_size = _cache_size        
        self.cache = []

    def commit(self, result):
        self.cache.append(self.values(**result))
        if len(self.cache) >= self.cache_size:
            self._commit()
    
    def _commit(self):
        self.con.executemany(self.insert_sql, self.cache)
        print >> sys.stderr, '%d records pushed'%(len(self.cache))
        self.cache = []
    
    def finish(self):
        self._commit()
        super(CachedCommitter, self).finish()


class Checker(object):
    
    def __init__(self):
        self.curl = pycurl.Curl()
        self.curl.setopt(pycurl.URL, "http://cet.99sushe.com/getscore.html")
        self.curl.setopt(pycurl.HTTPHEADER, ["Referer: http://cet.99sushe.com"])
        
        self.parser = re.compile(r"(?P<A>\d{1,3}),(?P<B>\d{1,3}),(?P<C>\d{1,3}),(?P<D>\d{1,3}),(?P<total>\d{1,3}),(?P<college>[^,]+),(?P<name>[^,]+)")
    
    def _check(self):
        buf = StringIO.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, buf.write)

        try:
            self.curl.perform()
            if (self.curl.getinfo(pycurl.HTTP_CODE) == 200):
                return buf.getvalue().decode("GB18030")
        except pycurl.error:
            pass
    
    def parse(self, tid, result):
        result = self.parser.match(result)
        if result:
            return dict(result.groupdict(), tid=tid)
    
    def check(self, tid):
        vc = get_vc(tid)
        if not vc: return

        self.curl.setopt(pycurl.POSTFIELDS, "id=%s&vc=%s"%(tid, vc))
        
        while True:
            result = self._check()
            if result is not None: break
            print >> sys.stderr, "error occured on", tid
        
        return self.parse(tid, result)
        

class CachedChecker(Checker):
    
    def __init__(self):
        super(CachedChecker, self).__init__()
        self._cache = {}
    
    def check(self, tid):
        if tid not in self._cache:
            result = super(CachedChecker, self).check(tid)
            if result is None: return None
            self._cache[tid] = result
        
        return self._cache[tid]
    
    def get_and_clear_cache(self):
        results = [ self._cache[key] for key in self._cache ]
        self._cache = {}
        return results
    

class CheckWorker(Worker):

    def __init__(self):
        super(CheckWorker, self).__init__()
        self.checker = CachedChecker()
    
    def work(self, task):
        (func, args, kwargs) = task
        tasks = func(self.checker, *args, **kwargs)
        results = self.checker.get_and_clear_cache()
        return (results, tasks if tasks else [])


class LazyList:

    def __init__(self, index_range, get_item):
        self.range = index_range
        self.get_item = get_item
        self._cached_item = [ None for i in index_range ]
        
    def __getitem__(self, index):
        if self._cached_item[index] is None:
           self._cached_item[index] = 0 if self.get_item(index) else 1
        
        return self._cached_item[index]
                   
    def binary_search(self):
        '''return index of the first empty item'''
        return bisect.bisect_left(self, 1, self.range[0], self.range[-1])


@task
def get_score(checker, prefix):
    checker.check(prefix)

@task
def get_room(checker, prefix):
    if checker.check("%s01"%(prefix)):
        tids = LazyList(range(1,32), lambda index:checker.check("%s%02d"%(prefix, index)))
        tid = tids.binary_search()
        return [ get_score("%s%02d"%(prefix, i))
                   for i in range(1, tid) if "%s%02d"%(prefix, i) not in checker._cache ]

@task
def get_place(checker, prefix):
    if checker.check("%s00101"%(prefix)):
        rooms = LazyList(range(1,1000), lambda index:checker.check("%s%03d01"%(prefix, index)))        
        room = rooms.binary_search()
        return [ get_room("%s%03d"%(prefix, i)) for i in range(1, room) ]


def main(database, cache_size, max_threads, *args):
    try:
        os.mkdir('vc')
    except OSError:
        pass

    committer = CachedCommitter(cache_size, database, 'result',
        tid = 'integer primary key',
        name = 'text', college = 'text', total = 'integer',
        A = 'integer', B = 'integer', C = 'integer', D = 'integer')
    collector = Collector(committer)
    queue = TaskQueue(collector, CheckWorker, max_workers=max_threads)
    
    for arg in args:
        
        if re.match(r"\d{6}102[12]\d{5}$", arg):
            queue.put(get_score(arg))
        
        elif re.match(r"\d{6}102[12]\d{3}$", arg):
            queue.put(get_room(arg))
        
        elif re.match(r"\d{6}102[12]\:\d{1,3}\-\d{1,3}$", arg):
            param = re.match(
                r"(?P<place>\d{6})102(?P<level>[12])\:(?P<from>\d{1,3})\-(?P<to>\d{1,3})",
                arg).groupdict()
            for i in range(int(param['from']), int(param['to'])+1):
                queue.put(get_room("%s102%s%03d"%(param['place'], param['level'], i)))
        
        elif re.match(r"\d{6}102[12]$", arg):
            queue.put(get_place(arg))
        
        elif re.match(r"\d{6}102([12])\-\d{6}102(\1)$", arg):
            param = re.match(
                r"(?P<from>\d{6})102(?P<level>[12])\-(?P<to>\d{6})102[12]",
                arg).groupdict()
            for i in range(int(param['from']), int(param['to'])+1):
                queue.put(get_place("%06d102%s"%(i, param['level'])))
        
        else:
            print >> sys.stderr, "Invalid ID: %s"%(arg)

    queue.join()
    
    if database == ':memory:':
        for result in committer.dump():
            print result


DESCRIPTION = '''

ID:
    tid:                                  XXXXXX102XXXXXX
    room:                                 XXXXXX102XXXX
    room range:                           XXXXXX102X:XXX-XXX
    place:                                XXXXXX102X
    place range:                          XXXXXX102X-XXXXXX102X'''

if __name__ == '__main__':
    from optparse import OptionParser, make_option
    
    parser = OptionParser(
        usage = "usage: %prog [options] id..." + DESCRIPTION,
        option_list =  [
            make_option("-k", "--threads", action="store", type="int", dest="N",
                default=10, help="set threads limit (default 10)"),
            make_option("-c", "--cache-size", action="store", type="int", dest="C", 
                default=0, help="numbers of records cached before commit (default 0)"),
            make_option("-o", "--database", action="store", type="string", dest="DB",
                default=':memory:', help="output database filename (default :memory:)")],
        epilog = '''Please report bugs to <bhuztez+cetbug@gmail.com>''')

    (options, args) = parser.parse_args()
    
    if len(args):  
        main(options.DB, options.C, options.N, *args)
    else:
        parser.print_help()

