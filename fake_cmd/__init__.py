"""
Some naming conventions:

- ``xxx_fp``: The real file path (not equal to the absolute path) 
that can be used to directly access the file.
- ``xxx_fname``: The file name.
- ``xxx_namespace``: A directory (or a directory name) that contains files.
- ``address``: Same to ``xxx_namespace``, but it is specially used 
for the main server namespace.
- ``terminate``: Denote to command-level terminate.
- ``destroy``: Denote to session-level destroy.
- About server and client naming: ``server`` means message sent to 
server, so does ``client``.
"""

# NOTE: Import ``readline`` here to improve input experience.
try:
    import readline
except Exception:
    pass
