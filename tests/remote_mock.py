home = [{"rel": "collection", "href": "/links", 
  "targetschema": { 
    "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/link"}]
      }
    }
  },
  {
    "rel": "link", "href": "/connects",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship"}]
      }
    }
  },
  {
    "rel": "link", "href": "/measures",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship"}]
      }
    }
  },
  {
    "rel": "link", "href": "/owns",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship"}]
      }
    }
  },
  {
    "rel": "link", "href": "/contains",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/nodes",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/node"},    
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/computenode"},
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/physicalnode"},
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/switchnode"}]
      }
    }
  },
  {
    "rel": "link", "href": "/runs",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/ports",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/port"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/services",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/service"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/measurements",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/measurements/measurement"}
        ]
      }
    }
  },
  {
    "rel": "collection", "href": "/metadata",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/measurements/metadata"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/exnodes",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/data/exnode"}]
      }
    }
  },
  {
    "rel": "collection", "href": "/allocations",
    "targetschema": {
      "type": "array",
      "items": {
        "type": "object",
        "oneOf": [{"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/data/allocation"},
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/data/ibp"},
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/data/ceph"},
          {"$ref": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/data/rdma"}]
      }
    }
  }
]

about={"schema_load_miss": 0,"ident": "6055593f-f713-49bf-aa8e-6205cb0e4389"}

nodes=[
    {
        ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/node",
        ":id": "1",
        ":ts": 5,
        "selfRef": "/nodes/1",
        "status": "UNKNOWN",
        "expires": 0
    },
    {
        ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/entities/topology/node",
        ":id": "2",
        ":ts": 3,
        "selfRef": "/nodes/2",
        "status": "UNKNOWN",
        "expires": 0
    }
]

links=[]


owns=[
    {
        ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship",
        ":id": "1",
        ":ts": 5,
        "subject": "nodes/1",
        "target":  "nodes/2"
    },
    {
        ":type": "http://unis.open.sice.indiana.edu/schema/2.0.0/relationship",
        ":id": "2",
        ":ts": 4,
        "subject": "nodes/2",
        "target":  "nodes/1"
    }
]
