'''
$Id$

A file based key-value store with transparent compression,
directory creation and file locking. 
Keys have file-based path semantics, values are the file contents.
Key names are not allowed to end in a '/'.

Future enhancements:
could do transparent (de)compression using magic values.
zlib compressed data starts with '\x78\x9c' and gzip compressed
data starts with '\x1f\x8b'.

Example usage:
>>> import tempfile
>>> import store
>>> location = tempfile.mkdtemp()
>>> name = 'level1/key/value'
>>> db = store.open(location)
>>> db.exists(name)
False
>>> obj = db.open(name, 'w') # open in write/truncate mode
>>> obj.write('ifInOctets 134184170.0 342031\\n')
30
>>> obj.close()
>>> db.exists(name)
True
>>> obj = db.open(name, 'a') # open in append mode
>>> obj.write('ifInOctets 134184189.0 342342\\n')
30
>>> obj.close()
>>> obj = db.open(name) # open in read mode
>>> obj.read()
'ifInOctets 134184170.0 342031\\nifInOctets 134184189.0 342342\\n'
>>> obj.close()
>>> g = db.find(path='level1') # return generator to list keys in level1 path
>>> for key in g:
...   print key
level1/key/value
>>> db.get(name)
'ifInOctets 134184170.0 342031\\nifInOctets 134184189.0 342342\\n'
>>> db.put(name, 'ifInOctets 134000000.0 340001')
>>> db.append(name, '\\nifInOctets 134000001.0 340002\\n')
>>> db.get(name)
'ifInOctets 134000000.0 340001\\nifInOctets 134000001.0 340002\\n'
>>> db.delete(name)
>>> list(db.find())
[]
>>> db.clean() # remove empty directories
>>> db.put('level1/foo/bar', 'testdata')
>>> db.drop('level1') # drop entire level1 container
>>> store.drop(location) # remove database
'''
import os
import fcntl
import errno
import re
import gzip
import shutil
import tempfile

COMPRESSLEVEL=1
COMPRESS=True

class Error(Exception):
  pass

class Invalid(Exception):
  pass

class Locked(Error):
  pass

class DoesNotExist(Error):
  pass

class NotAKey(Error):
  pass

class NotAContainer(Error):
  pass

def _check_error(key, exc):
  if exc.errno == errno.ENOENT:
    raise DoesNotExist('key %s does not exist' % key)
  if exc.errno == errno.EISDIR:
    raise NotAKey('"%s" is not a key' % key)
  if exc.errno == errno.ENOTDIR:
    raise NotAContainer('"%s" is not a container' % key)

def _lock(fileobj):
  # file should also be opened in write mode, because on some platforms
  # this is required to make locking work.
  try:
    fcntl.lockf(fileobj, fcntl.LOCK_EX|fcntl.LOCK_NB)
  except IOError, e:
    if e.errno in (errno.EACCES, errno.EAGAIN):
      raise Locked('file "%s" is locked' % fileobj.name)
    else:
      raise

def _unlock(fileobj):
  fcntl.lockf(fileobj, fcntl.LOCK_UN)

def _makedirs(filename):
  'Create intermediate directories.'
  if os.path.exists(filename):
    return

  dirname, basename = os.path.split(filename)
  if not dirname:
    raise Error('expected a directory in "%s"' % filename)
  if not basename:
    raise Error('expected a filename in "%s"' % filename)
  if not os.path.exists(dirname):
    os.makedirs(dirname)

class Database(object):
  def __init__(self, location):
    'Open database at specified location.'
    self.location = os.path.abspath(location)

  def _within_base(self, filename):
    return os.path.commonprefix((self.location, filename)) == self.location

  def _make_path(self, path):
    if path.startswith('/'):
      path = path[1:] # make relative path
    dst = os.path.abspath(os.path.join(self.location, path))
    if not self._within_base(dst):
      raise Invalid('invalid path "%s"' % path)

    return dst

  def get_fullname(self, path):
    'Return fullname of `path`.'
    if path.endswith('/'):
      raise Invalid('path may not end with a "/"')
    return self._make_path(path)

  def exists(self, path):
    'Test if key or container at `path` exists.'
    return os.path.exists(self.get_fullname(path))

  def is_key(self, path):
    'Test if `path` is a key.'
    return os.path.isfile(self.get_fullname(path))

  def is_container(self, path):
    'Test if `path` is a container.'
    return os.path.isdir(self.get_fullname(path))

  def getmtime(self, path):
    'Return modification time for `path`.'
    try:
      return os.path.getmtime(self.get_fullname(path))
    except (IOError, OSError), e:
      _check_error(path, e)
      raise Error('could not get mtime for %s (%s)' % (path, e))

  def getsize(self, path):
    'Return size in bytes of contents of `path`.'
    try:
      return os.path.getsize(self.get_fullname(path))
    except (IOError, OSError), e:
      _check_error(path, e)
      raise Error('could not get size for %s (%s)' % (path, e))

  def create(self, path=None, compress=COMPRESS, compresslevel=COMPRESSLEVEL, prefix='tmp', suffix=''):
    'Create a new key, within path if specified. Return key and file object.'
    try:
      dst = self.location
      if path:
        dst = self.get_fullname(path)
      if not os.path.exists(dst):
        os.makedirs(dst)
      if compress and not suffix:
        suffix = '.gz'
      fd, filename = tempfile.mkstemp(dir=dst,prefix=prefix,suffix=suffix)
    except (IOError, OSError), e:
      raise Error('could not create key within %s (%s)' % (dst, e))

    key = filename[len(self.location)+1:]
    try:
      f = os.fdopen(fd, 'w+')
      _lock(f)
      if compress:
        return key, gzip.GzipFile(fileobj=f, mode='w+', compresslevel=compresslevel)
    except (IOError, OSError), e:
      _check_error(key, e)
      raise Error('could not create key %s (%s)' (key, e))
    return key, f

  def create_container(self, path):
    'Create a new container at `path`.'
    if self.exists(path):
      if self.is_container(path):
        raise Error('container at %s already exists' % path)
      raise Error('path %s already exists but is not a container' % path)
    try:
      os.makedirs(self.get_fullname(path))
    except (IOError, OSError), e:
      raise Error('could not create container at path %s (%s)' % (path, e))

  def open(self, key, mode='r', compress=COMPRESS, compresslevel=COMPRESSLEVEL):
    'Open `key` and return a file object.'
    assert mode in ('r','w','a','r+','w+'), 'Invalid mode (%s)' % mode
    filename = self.get_fullname(key)
    try:
      if mode != 'r':
        _makedirs(filename)
      f = file(filename, mode + 'b')
      if mode != 'r':
        _lock(f)
      if compress:
        return gzip.GzipFile(fileobj=f, mode=mode, compresslevel=compresslevel)
    except (IOError, OSError), e:
      _check_error(key, e)
      raise Error('could not open key %s (%s)' % (key, e))
    return f

  def delete(self, key):
    'Delete key `key`.'
    try:
      os.remove(self.get_fullname(key))
    except (IOError, OSError), e:
      _check_error(key, e)
      raise Error('could not delete key %s (%s)' % (key, e))

  def drop(self, path):
    'Drop container at `path`.'
    drop(self.get_fullname(path))

  def clean(self):
    'Remove empty directories in db.'
    to_remove = []
    for dirpath, dirnames, filenames in os.walk(self.location):
      if not (dirnames or filenames):
        to_remove.append(dirpath)
    for path in to_remove:
      if os.path.exists(path):
        os.removedirs(path)
    if not os.path.exists(self.location):
      # re-create it
      os.makedirs(self.location)

  def find(self, pattern='.*', path=None, match_path=True):
    '''Return a generator object to list existing keys matching pattern.

    Pattern should be a regular expression string. If pattern is omitted, list all
    existing keys.
    If path is provided, use it as a starting point for the search instead of the top
    database path. This can improve performance on large databases.
    If match_path is True (default), include the path in the pattern match. Otherwise
    only match last (non-path) part of the key.
    '''
    if path is None:
      dst = self.location
    else:
      dst = self.get_fullname(path)

    l = len(dst)
    RE = re.compile(pattern)
    for dirpath, dirnames, filenames in os.walk(dst):
      d = dirpath[l+1:]
      if path:
        d = os.path.join(path, d)
      for filename in filenames:
          tbl = os.path.join(d, filename)
          if match_path:
            filename = tbl
          if RE.match(filename):
            yield tbl

  def rename(self, src, dst):
    'Rename key `src` to `dst`.'
    # we assume src exists
    try:
      src_filename = self.get_fullname(src)
      dst_filename = self.get_fullname(dst)
      _makedirs(dst_filename)
      os.rename(src_filename, dst_filename)
    except (IOError, OSError), e:
      _check_error('%s or %s' % (src, dst), e)
      raise Error('could not rename %s to %s (%s)' % (src, dst, e))
      
  def put(self, key, value, compress=COMPRESS):
    'Set value of `key` to `value`.'
    try:
      f = self.open(key, 'w', compress=compress)
      f.write(value)
      f.close()
    except (IOError, OSError), e:
      raise Error('could not put key %s (%s)' % (key, e))

  def append(self, key, value, compress=COMPRESS):
    'Append `value` to end of current value of `key`.'
    try:
      f = self.open(key, 'a', compress=compress)
      f.write(value)
      f.close()
    except (IOError, OSError), e:
      raise Error('could not append to key %s (%s)' % (key, e))

  def get(self, key, compress=COMPRESS):
    'Return value for key `key`.'
    try:
      # file must exist, and open in read-write mode
      # so it will be locked for writing.
      f = self.open(key, 'r+', compress=compress)
      s = f.read()
      f.close()
      return s
    except (IOError, OSError), e:
      raise Error('could not get key %s (%s)' % (key, e))

def open(location):
  'Open database at location and return a Database object.'
  return Database(location)

def drop(location):
  'Remove database at location.'
  try:
    shutil.rmtree(location)
  except (IOError, OSError), e:
    _check_error(location, e)
    raise Error('could not drop location %s (%s)' % (location, e))

def create(path=None):
  'Create a temporary database within path if specified. Return location and Database object.'
  try:
    location = tempfile.mkdtemp(dir=path)
    return location, Database(location)
  except (IOError, OSError), e:
    raise Error('could not create database (%s)' % e)

def _test():
  import doctest
  doctest.testmod()

if __name__ == "__main__":
  _test()
