#Base script written by David Smith, updated by Matt Doidge
#sets up PATHS for the tarball, source from /etc/profile.d/
#v8 update, 30/5/2013

#point this at your tarball base directory (cvmfs is just being used as a lazy example)
cvmfs_base=/cvmfs/grid.cern.ch
base="$cvmfs_base"/emi-wn-3.7.3-1_sl6v2

#EMI_TARBALL_BASE flag to let jobs know that this is a tarball node
#also used to locate etc/emi-version file by SAM nagios probes

export EMI_TARBALL_BASE=$base

# site vo/specific (example value invalid or needs further setup)
# you may wish to set these in a different script for clarity.
#export VO_MYVO_SW_DIR=/experiment/myvo
#export VO_MYVO_DEFAULT_SE=se1
#export DPM_HOST=thedpm.example.com
#export DPNS_HOST=thedpm.example.com
#export SITE_GIIS_URL=mybdii.example.com
export X509_CERT_DIR=${cvmfs_base}/etc/grid-security/certificates
export X509_VOMS_DIR=${base}/etc/grid-security/vomsdir
export VOMS_USERCONF=${base}/etc/vomses
export SITE_NAME=RRC-KI-HPC2

# site specific (with default)
# set these too someting more appropriate to your site/region
export MYPROXY_SERVER=myproxy.cern.ch
export LCG_GFAL_INFOSYS=truth.grid.kiae.ru:2170
export BDII_LIST=truth.grid.kiae.ru:2170

# not site specific; usually no change needed
#
export -n GRID_ENV_LOCATION=
export -n GLITE_ENV_SET=
export GT_PROXY_MODE=old

#note that if the "base" LD_LIBRARY_PATH isn't set elsewhere you need to
#set it here, so that it also points to the local /lib, /lib64, /usr/lib,
#/usr/lib64 directories
v="$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH=${base}/lib64:${base}/lib:${base}/usr/lib64:${base}/usr/lib
#if your site uses gsi dcap you will need to add to the LD_LIBRARY_PATH :${base}/usr/lib64/dcap
if [ -n "$v" ]; then
  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$v"
fi
#similar to LD_LIBRARY_PATH, if the PATH is not set elsewhere the "base elements" (/bin, /sbin etc)
#will need to be set here
export PATH=${base}/bin:${base}/sbin:${base}/usr/bin:${base}/usr/sbin:$PATH

v="$MANPATH"
export MANPATH=${base}/usr/share/man
if [ -n "$v" ]; then
  export MANPATH="${MANPATH}:$v"
fi

v="$PERL5LIB"
export PERL5LIB=${base}/usr/lib64/perl5/vendor_perl:${base}/usr/lib/perl5/vendor_perl
if [ -n "$v" ]; then
  export PERL5LIB="${PERL5LIB}:$v"
fi
v="$PYTHONPATH"
#it's important that there is no trailing / for the PYTHONPATH variable so python loads the setup script
#correctly
export PYTHONPATH=${base}/usr/lib64/python2.6/site-packages:${base}/usr/lib/python2.6/site-packages

#some sites might need to explicitly expand the tarball base if
#their users alter the PYTHONPATH significantly, for example:
#$base/usr/lib64/python2.6:$base/usr/lib64/python2.6/site-package
#see the PYTHONPATH section of the documentation for more details.
#SL6 users have it easier as there is only one version of python to worry about

if [ -n "$v" ]; then
  export PYTHONPATH="${PYTHONPATH}:$v"
fi

#if java is installed on the node it's advisable to use that version instead
#it's also worth checking to see if the version of java has changed within the
#tarball, although this should be correct - as of EMI3 the "tarball" java is in the
#os-extras tarball
export JAVA_HOME=${base}/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre

export LCG_LOCATION=${base}/usr
export GLITE_LOCATION=${base}/usr
export GLITE_LOCATION_VAR=/var
export SRM_PATH=${base}/usr/share/srm
unset v base

