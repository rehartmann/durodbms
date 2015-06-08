import os

Alias('all', 'dreisam.fcgi')

env = Environment(ENV = os.environ, tools = ['default', 'textfile'])

duro_home = '/usr/local/durodbms.1.0'
bdb_home = '/usr/local/BerkeleyDB.6.1'

if (os.environ.has_key('CFLAGS')):
    env.Replace(CCFLAGS = os.environ['CFLAGS'])

dreisam = env.Program('dreisam.fcgi', ['src/dreisam.c', 'src/getaction.c',
                      'src/viewop.c', 'src/sreason.c'],
            CPPPATH = [bdb_home + '/include',
                       duro_home + '/include'],
            LINKFLAGS = ['-g'],
            LIBPATH = [bdb_home + '/lib',
                       duro_home + '/lib'],
            LIBS = ['fcgi', 'duro', 'db'],
            RPATH = [duro_home + '/lib', bdb_home + '/lib'])

drtest = env.Program('drtest', ['src/drtest.c'],
            CPPPATH = [bdb_home + '/include',
                       duro_home + '/include'],
            LINKFLAGS = ['-g'],
            LIBPATH = [bdb_home + '/lib',
                       duro_home + '/lib'],
            LIBS = ['fcgi', 'duro', 'db'],
            RPATH = [duro_home + '/lib', bdb_home + '/lib'])

doc_root = '/var/www'

fcgi_root = doc_root + '/fcgi-bin'

Alias('deploy', fcgi_root)

Install(fcgi_root, dreisam)
