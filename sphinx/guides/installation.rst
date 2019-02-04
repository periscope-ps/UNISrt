############
Installation
############

*******
Centos7
*******

0. Install setuptools
---------------------

The runtime requires Python3.  For RHEL based systems, this means installing
python3 manually.  If you do not already have python3 on your system, follow
the directions found here: https://linuxize.com/post/how-to-install-python-3-on-centos-7/

If your system does not already have setuptools, first install pip::

  $ wget https://bootstrap.pypa.io/get-pip.py
  $ sudo python get-pip.py


1. Clone the Repository
-----------------------

``git clone https://github.com/periscope-ps/UNISrt.git``

2. Install Software
-------------------

``sudo python3 setup.py build install``

2. (optional) Install Software as Develop
-----------------------------------------

Installing as develop - in place of the above step - allows a
developer to modify the runtime code and evaluate the change without
rebuilding the project.

``sudo python3 setup.py develop``
