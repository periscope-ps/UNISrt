.. UNISrt documentation

..image:: _static/CREST.png
     :align: center

UNISrt
==================

The Unified Network Information Service, UNIS, provides service discovery and metadata, describing and enabling the discovery of IBP depots, perfSONAR and Periscope services as well as Phoebus gateways. It is a distributed service with a REST interface which can hold a variety of information about a network. This includes information about the topology of the network including hosts, switches, routers, network links, ports etc. In addition, UNIS stores and serves information about a network, including things like instances of BLIPP, measurement stores, and perfSONAR measurement points. UNIS stores measurement metadata and information abou the "performance topology" of the network that is derived from ongoing measurements. Finally UNIS stores DLT exnodes describing the data distribution of file objects.

.. image:: _static/NMAL-Unis.png
    :align: center

.. toctree::
   :maxdepth: 2

   getting_started.rst
