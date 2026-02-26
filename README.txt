This script is run daily and checks if CBS published new Iv3 records which we use for Open Spending.
If this is the case an email will be sent to alert the developers that new new data is available.

A normal problem is with Gemeenschappelijke Regelingen (the error happens in the `save_government_model` step),
we often can't find an address automatically for a new Gemeenschappelijke Regeling (this usually happens in May when
the first quarter of the new year is published?) so we have to add the address manually to
`openspending-interface/base/data/GR_locations.json`. Check kvk.nl and search for the name of the
Gemeenschappelijke Regeling to find its postal code and city. Afterwards you can manually run this one command
again to see if there are other new GRs which you need to add (instead of running the whole process which takes much longer).


Some other random problems which were unforseen:
- CBS used 'GR0003.' as an ID, the '.' had to be removed
- CBS used the description (which is the long title) for two GRs, one being correct and one wrong. In this case you will get an error during the import_cbs_data step as it finds 2 governments with the same code (this should be fixed by not allowing the save_government step to add a government whose code is already present instead of allowing unique combinations of name and code). Go to openspending.nl/admin, log in and remove the wrongly added new government. Then run the import_cbs_data command and the following download_metrics command (check the check-openspending.py script for the commands)
