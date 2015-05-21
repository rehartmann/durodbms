import os

Alias('all', 'dreisam.fcgi')

env = Environment(ENV = os.environ, tools = ['default', 'textfile'])

if (os.environ.has_key('CFLAGS')):
    env.Replace(CCFLAGS = os.environ['CFLAGS'])

dreisam = env.Program('dreisam.fcgi', ['src/dreisam.c', 'src/getaction.c', 'src/viewop.c'],
            CPPPATH = ['/usr/local/BerkeleyDB.6.1/include',
                       '/usr/local/durodbms.0.26/include'],
            LINKFLAGS = ['-g'],
            LIBPATH = ['/usr/local/BerkeleyDB.6.1/lib',
                       '/usr/local/durodbms.0.26/lib'],
            LIBS = ['fcgi', 'duro', 'db'],
            RPATH = ['/usr/local/durodbms.0.26/lib', '/usr/local/BerkeleyDB.6.1/lib'])

drtest = env.Program('drtest', ['src/drtest.c'],
            CPPPATH = ['/usr/local/BerkeleyDB.6.1/include',
                       '/usr/local/durodbms.0.26/include'],
            LINKFLAGS = ['-g'],
            LIBPATH = ['/usr/local/BerkeleyDB.6.1/lib',
                       '/usr/local/durodbms.0.26/lib'],
            LIBS = ['fcgi', 'duro', 'db'],
            RPATH = ['/usr/local/durodbms.0.26/lib', '/usr/local/BerkeleyDB.6.1/lib'])

doc_root = '/var/www'

fcgi_root = doc_root + '/fcgi-bin'

Alias('deploy', fcgi_root)

Install(fcgi_root, dreisam)
