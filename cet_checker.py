#!/usr/bin/env python
#   cet_checker.py - check CET score(s)
#   Copyright (C) 2009  bhuztez <bhuztez@gmail.com>
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

Check your CET score directly without unnecessary waiting, or 
speed up checking of many many scores with multi-thread.Do not 
set thread limit too high, which will cause too many error when 
connecting to the server and slow down your checking
"""
__author__  = 'bhuztez <bhuztez@gmail.com>'
__version__ = '0.01.0906'

from Queue import Queue
import sys

class worker:
    '''fetch and do work '''
    
    def __init__(self, queue, context):
        '''start worker thread'''
        
        from threading import Thread
        
        self.queue = queue
        self.context = context
        self.thread = Thread(target=lambda:self.run())
        self.thread.setDaemon(True)
        self.thread.start()
        
    def run(self):
        '''do work self.queue is not empty'''
        
        while not self.queue.empty():            
            work = self.queue.get()
            work.run(self.queue, self.context)
            
            self.queue.task_done()
        else:
            self.queue.worker_quit()

class work_queue(Queue):
    '''
    queue of work
    It is not simply a queue, it will start worker thread on demand ^_^
    
    Usage:
      q = work_queue(context_builder, write_result)
      work = yourwork
      q.put(work)
      q.join()
    '''
    
    def __init__(self,
                 context_builder, write_result,
                 max_threads = 10, maxsize = 0):
        '''
        context_builder - work.run(queue, context)
        max_threads - maximum number of threads queue can start
        write_result - redirect result to your function object
            def write_result(result):
                do_something_with(result)
                
          if not provided, result will print to screen directly
        '''
        
        from threading import Lock
        
        Queue.__init__(self, maxsize)
        self.lock = Lock()
        self.thread_count = 0
        self.max_threads = max_threads
        self.context_builder = context_builder
        self.write_result = write_result
        
    def put(self, item, block = True, timeout = None):
        '''start a worker thread if not reach thread limit'''
        
        Queue.put(self, item, block, timeout)
        if self.get_worker_count() < self.max_threads:
            self.start_worker()
        
    def start_worker(self):
        '''start a worker thread'''
        
        w = worker(self, self.context_builder())
        self.lock.acquire()
        self.thread_count = self.thread_count + 1
        self.lock.release()
    
    def worker_quit(self):
        '''worker thread MUST call this method when quit'''
        
        self.lock.acquire()
        self.thread_count = self.thread_count - 1
        self.lock.release()
        
    def get_worker_count(self):
        '''return number of threads currently working'''
        
        self.lock.acquire()
        count = self.thread_count
        self.lock.release()
        return count
        
    def commit(self, result):
        '''to be called by worker thread'''
        
        self.write_result(result)

class db_daemon:
    '''store results in sqlite3 database'''
    
    def __init__(self,
                 print_result,
                 sql_create, sql_insert, sql_dump,
                 database = None, append = False):
        
        from sqlite3 import connect, Row
        from threading import Event
        
        self.print_result = print_result
        self.sql_insert = sql_insert
        self.sql_dump = sql_dump
        
        if database:
            self.con = connect(database)
            if not append:
                self.con.execute(sql_create)
        else:
            self.con = connect(":memory:")
            self.con.execute(sql_create)
        self.con.row_factory = Row
        
        self.queue = Queue()
        self.event = Event()
        
        
    def insert(self, result):
        '''insert a result into database'''
        
        self.queue.put(result)
        if not self.event.isSet():
            self.event.set()
        
    def serve(self, is_finished, cache_size = 0):
        '''store received data until finished'''
        
        from sqlite3 import IntegrityError
        
        cache = []
        count = 0
        
        while True:
            if self.queue.empty():
                if is_finished():
                    if len(cache) > 0:
                        try:
                            self.con.executemany(self.sql_insert, cache)
                        except IntegrityError, err:
                            print >> sys.stderr, "duplicate record(s)"
                    self.con.commit()
                    break
                else:
                    self.event.clear()
                    self.event.wait(10)
                    self.event.set()
                    continue
                
            result = self.queue.get()
            
            try:
                if cache_size > 0:
                    cache.append(result)
                    if len(cache) >= cache_size:
                        self.con.executemany(self.sql_insert, cache)
                        self.con.commit()
                        count = count + cache_size
                        print >> sys.stderr, "stored", count, "records"
                        cache = []
                else:
                    self.con.execute(self.sql_insert, result)
            except IntegrityError, err:
                print >> sys.stderr, "duplicate record(s)"
                
            self.queue.task_done()
    
    def dump(self):
        '''print out result to screen'''
        
        for row in self.con.execute(self.sql_dump):
            self.print_result(row)

# 0906-specific followed

class result_checker:
    '''
    check result
    
    Usage:
        checker = result_checker(
            convert = lambda string:convert_to_codec('UTF-8', string))
        result = checker.check(tid)
        if checker.match(result):
            print result
        else:
            print "error"
    '''
    
    URL = "http://cet.99sushe.com/getscore.html"
    extra_headers = ["Referer: http://cet.99sushe.com"]
    result_format = r"\d{1,3},\d{1,3},\d{1,3},\d{1,3},\d{1,3},[^,]+,[^,]+"
    result_parser = r"(?P<A>\d{1,3}),(?P<B>\d{1,3}),(?P<C>\d{1,3}),(?P<D>\d{1,3}),(?P<total>\d{1,3}),(?P<college>[^,]+),(?P<name>[^,]+)"
    
    @staticmethod
    def convert_to_codec(codec, string):
        '''convert/encode result string to codec'''
        
        return unicode(string, "GB18030").encode(codec)
    
    @staticmethod
    def format_result(result):
        return "%s,%3d,%3d,%3d,%3d,%3d,%s,%s"%(
            result['tid'],
            int(result['A']),
            int(result['B']),
            int(result['C']),
            int(result['D']),
            int(result['total']),
            result['college'],
            result['name'])
    
    def __init__(self, convert=None):
        '''
        initialize a result_checker
        
        convert - function object like followed:
            def convert(string):
                \'\'\'string - raw string received from server\'\'\'
                return do_something_with(string)
        
          if not provided, check method will return raw string
        '''
        
        from pycurl import Curl, URL, HTTPHEADER
        import re
        self.curl = Curl()
        self.curl.setopt(URL, result_checker.URL)
        self.curl.setopt(HTTPHEADER, result_checker.extra_headers)
    
        self.convert = convert
        self.regex = re.compile(result_checker.result_format)
        self.parser = re.compile(result_checker.result_parser)
    
    def check(self, tid):
        '''check cet-score of a specific tid'''
        
        from StringIO import StringIO
        from pycurl import POSTFIELDS, WRITEFUNCTION, HTTP_CODE, error
        self.curl.setopt(POSTFIELDS, "id=%s"%(tid))
        buf = StringIO()
        self.curl.setopt(WRITEFUNCTION, buf.write)
    
        succeed = False
        try:
            self.curl.perform()
            succeed = (self.curl.getinfo(HTTP_CODE) == 200)
        except error:
            pass
    
        while not succeed:
            print >> sys.stderr, "error occured on", tid
            buf = StringIO()
            self.curl.setopt(WRITEFUNCTION, buf.write)
            try:
                self.curl.perform()
                succeed = (self.curl.getinfo(HTTP_CODE) == 200)
            except error:
                pass
        
        if self.convert:
            result = self.convert(buf.getvalue())
        else:
            result = buf.getvalue()

        return result
        
    def validate(self, string):
        '''check if string is a valid result'''
        
        return self.regex.match(string)
        
    def parse(self, tid, string):
        '''parse result'''
       
        result = self.parser.match(string).groupdict()
        if result:
            result['tid'] = tid
        return result

class lazy_list:
    '''
    fetch result on request to reduce latency
    
    see source code for detail
    '''
    
    def __init__(self, index_range, get_item):
        self.range = index_range
        self.get_item = get_item
        
    def __getitem__(self, index):
        '''
        return item of index
        
        to be called by bisect.bisect_left
        see source code in detail
        '''
        if index in self.range:
            if self.get_item(index):
                return 0
            else:
                return 1
        else:
            raise IndexError
                
    def binary_search(self):
        '''return index of the first empty item'''
        
        from bisect import bisect_left
        return bisect_left(self, 1, self.range[0], self.range[-1])

class work:
    '''
    the work object
    
    Usage:
      w = work(prefix, work.get_score)
      w.run(queue, checker)
      
      
    see source code and class work_queue for detail
    '''
    
    @staticmethod
    def try_place(queue, checker, prefix):
        '''put works of the place with prefix to queue'''
        
        if checker.check("%s00101"%(prefix)):
            room_list = lazy_list(range(1,1000),
                lambda index:checker.check("%s%03d01"%(prefix, index)))
                
            room = room_list.binary_search()
            
            for i in range(1, room):
                queue.put(work("%s%03d"%(prefix, i), work.try_room))
    
    @staticmethod
    def try_room(queue, checker, prefix):
        '''put works of the room with prefix to queue'''
        
        if checker.check("%s01"%(prefix)):
            tid_list = lazy_list(range(1,32),
                lambda index:checker.check("%s%02d"%(prefix, index)))
            
            tid = tid_list.binary_search()
            
            for i in range(1, tid):
                queue.put(work("%s%02d"%(prefix, i), work.get_score))
    
    @staticmethod
    def get_score(queue, checker, prefix):
        '''get score of tid specified in prefix'''
        
        result = checker.check(prefix)
        queue.commit(checker.parse(prefix, result))
    
    def __init__(self, prefix, work):
        '''prefix - prefix or id'''
        
        self.prefix = prefix
        self.work = work
        
    def run(self, queue, checker):
        '''do work'''
        
        self.work(queue, checker, self.prefix)

def usage():
    '''print usage'''
    
    print "Usage:", sys.argv[0],"[options] id..."
    print '''    
options:
    -k<N>, --threads=<N>                  set threads limit to N (default 10)
    -o <DATABASE>, --output=<DATABASE>    output result to sqlite3 DATABASE 
    --memdb                               equivalent to --output=:memory:
    -a <DATABASE>, --append=<DATABASE>    append result to sqlite3 DATABASE
    -c <N>, --cache=<N>                   cache N records before commit to db
    
id:
    tid:                                  XXXXXX091XXXXXX
    room:                                 XXXXXX091XXXX
    room range:                           XXXXXX091X:XXX-XXX
    place:                                XXXXXX091X
    place range:                          XXXXXX091X-XXXXXX091X

tips:
    Use shell scripts to seperate your task into several sub-tasks to save
    memory, if you are working with a large amount of scores
    
Please report bugs to <bhuztez@gmail.com>'''

def main(args, codec, max_threads = 10, 
         database = None, cache_size = 0, append = False):
    
    from re import match
    convert = lambda string: result_checker.convert_to_codec(codec, string)
    
    if database:
      db = db_daemon(
          lambda result: 
              sys.stdout.write("%s\n"%(
                  result_checker.format_result(result).encode(codec))),
           '''create table cet_result (
               tid      integer primary key,
               name     text,
               college  text,
               total    integer,
               A        integer,
               B        integer,
               C        integer,
               D        integer)''',
            '''insert into
                cet_result(tid, name, college, total, A, B, C, D) 
                values    (  ?,    ?,       ?,     ?, ?, ?, ?, ?)''',
            '''select * from cet_result''',
          database,
          append)
      q = work_queue(
          lambda :
              result_checker(convert),
          lambda result:
              db.insert((
                  result['tid'],
                  result['name'],
                  result['college'],
                  result['total'],
                  result['A'],
                  result['B'],
                  result['C'],
                  result['D'])),
          max_threads)
    else:
      q = work_queue(
          lambda:
              result_checker(convert),
          lambda result: 
              sys.stdout.write("%s\n"%(result_checker.format_result(result))),
          max_threads)
    
    for arg in args:
        if match(r"\d{6}091[12]\d{5}$", arg):
            # check tid
            result = result_checker(convert).check(arg)
            if result:
                print result
            else:
                print >> sys.stderr, "NO record!"
        
        elif match(r"\d{6}091[12]\:\d{1,3}\-\d{1,3}$", arg):
            # check room range
            param = match(
                r"(?P<place>\d{6})091(?P<level>[12])\:(?P<from>\d{1,3})\-(?P<to>\d{1,3})",
                arg).groupdict()
            for i in range(int(param['from']), int(param['to'])+1):
                q.put(
                    work("%s091%s%03d"%(param['place'], param['level'], i), 
                         work.try_room))
            
        elif match(r"\d{6}091[12]\d{3}$", arg):
            # check room
            q.put(work(arg, work.try_room))
            
        elif match(r"\d{6}091([12])\-\d{6}091(\1)$", arg):
            #check place range
            param = match(
                r"(?P<from>\d{6})091(?P<level>[12])\-(?P<to>\d{6})091[12]",
                arg).groupdict()
            for i in range(int(param['from']), int(param['to'])+1):
                q.put(work("%06d091%s"%(i, param['level']), work.try_place))
            
        elif match(r"\d{6}091[12]$", arg):
            #check place
            q.put(work(arg, work.try_place))
        
        else:
            print >> sys.stderr, "ignored invalid arg:", arg
    
    if database:
        db.serve(lambda: q.get_worker_count()==0, cache_size)
        q.join()
        if database == ":memory:":
            db.dump()
    else:
        q.join()

if __name__ == '__main__':
    from getopt import getopt, GetoptError
    try:
        max_threads = 10
        database = None
        cache_size = 0
        append = False
        opts, args = getopt(
            sys.argv[1:],
            "c:o:a:k:",
            ["threads=","memdb","output=","append=","cache="])
        
        for o, a in opts:
            if o in ['-k', '--threads']:
                max_threads = int(a)
            elif o in ['--memdb']:
                if not database:
                    database = ":memory:"
                    append = False
                else:
                    raise GetoptError('database conflict')
            elif o in ['-o', '--output']:
                if not database:
                    database = a
                    append = False
                else:
                    raise GetoptError('database conflict')
            elif o in ['-a', '--append']:
                if not database:
                    database = a
                    append = True
                else:
                    raise GetoptError('database conflict')
            elif o in ['-c', '--cache']:
                cache_size = int(a)
        
        if len(args)>0:
            main(args, "UTF-8", max_threads, database, cache_size, append)
        else:
            usage()
       
    except GetoptError, err:
        print "GetoptError:", str(err)
        sys.exit(2)
    except ValueError, err:
        print "ValueError:", str(err)
        sys.exit(2)
    
