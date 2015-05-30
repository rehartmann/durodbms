#
# Set up directories and database for the suppliers example.
# This script must be run from the directory it is in.
#
# Afterwards, the database environment directory (/var/www/fcgi-bin/dreisam/data/dbenv by default)
# and all files in it must be made readable and writable by the webserver.
# The most reliable way to ensure this is to make the directory and
# its files owned by the user under which the webserver runs. (e.g. www-data)
#

DURODBMS_ROOT=/usr/local/durodbms.0.26
DURODT=$DURODBMS_ROOT/bin/durodt

WWW_ROOT=/var/www
FCGI_DIR=$WWW_ROOT/fcgi-bin
DREISAM_DIR=$FCGI_DIR/dreisam
DATA_DIR=$DREISAM_DIR/data
DB_ENV_DIR=$DATA_DIR/dbenv
VIEWS_DIR=$DREISAM_DIR/views

SUPP_DIR=..
DREISAM_PROJ_DIR=$SUPP_DIR/../..

# Create directories
mkdir $DREISAMDIR $DATA_DIR $VIEWS_DIR

# Copy templates and HTML files
cp $SUPP_DIR/views/*.thtml $VIEWS_DIR
cp $SUPP_DIR/www-static/*.html $WWW_ROOT

# Create database environment
echo "create_env('$DB_ENV_DIR');" | $DURODT

# Create suppliers database and tables
$DURODT -e $DB_ENV_DIR $SUPP_DIR/td/db.td

# Create Dreisam tables and operators
$DURODT -e $DB_ENV_DIR -d SDB $DREISAM_PROJ_DIR/td/setup.td
$DURODT -e $DB_ENV_DIR -d SDB $DREISAM_PROJ_DIR/td/template.td

# Create action operators
$DURODT -e $DB_ENV_DIR -d SDB $SUPP_DIR/td/actions.td

# Copy config file
cp $SUPP_DIR/td/config.td $DREISAM_DIR
