"""
Use fabric to run comamnds on remote via ssh
"""

from fabric import Connection

cxn_name = 'bumblebaby'
c = Connection(host = cxn_name, user = 'abuzarmahmood')

result = c.run(
        """
        source ~/anaconda3/bin/activate && 
        conda activate blech_clust && 
        which python
        """, 
        pty=True)
