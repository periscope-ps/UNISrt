# Instructions

1. clone this repo: https://github.com/periscope-ps/UNISrt
2. `pip install virtualenv` or pip3 if you have it
3. `virtualenv -p /your/python/version venv`  where `venv` can be any name
   for example `virtualenv -p /usr/local/bin/python3 venv`
4. `source venv/bin/activate`
5. `python -V` to confirm that you're using the same python as specified in step 3.
6. `python setup.py build install test` should do a bunch of things

# Usage

```python
from unis import Runtime

rt = Runtime("http://unis.crest.iu.edu:8888")
rt.metadata  # you can `dir(rt)` this to see more

# example: print metadata
for md in rt.metadata:
    print md.name  # rt has "reference-like" behavior, so be careful...
    # md.name = random()  # ...because it's easy to clobber someone else's work (don't do this!)
    
# example: print each port within each node
for n in rt.nodes:
    print(n.name)
    for p in n.ports:
            print("\t",p.name)
    print("\n")
```
