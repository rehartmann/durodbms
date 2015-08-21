import os
import glob

release = '0.1'

Alias('all', 'dreisam.fcgi')

env = Environment(ENV = os.environ, tools = ['default', 'textfile'])

duro_home = '/usr/local/durodbms.1.0'
bdb_home = '/usr/local/BerkeleyDB.6.1'

if (os.environ.has_key('CFLAGS')):
    env.Replace(CCFLAGS = os.environ['CFLAGS'])

dreisam_src = ['src/dreisam.c', 'src/getaction.c', 'src/viewop.c',
               'src/sreason.c', 'src/json.c'];

dreisam = env.Program('dreisam.fcgi', dreisam_src,
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

if env.has_key('TAR'):
    disttar = 'dreisam-' + release + '-src.tar.gz'
    env.Tar(disttar, dreisam_src + glob.glob('src/*.h')
            + glob.glob('docs/*.html') + glob.glob('docs/style.css')
            + glob.glob('td/*.td') + glob.glob('supp/td/*.td')
            + glob.glob('examples/supp/views/*.thtml')
            + glob.glob('examples/supp/www-static/*.html')
            + glob.glob('examples/supp/td/*.td')
            + glob.glob('examples/supp/sh/*.sh')
            + ['COPYING', 'INSTALL', 'README', 'SConstruct'],
            TARFLAGS = '-c -z --transform \'s,^,dreisam-' + release + '/,S\'')
    env.Alias('dist', disttar)
