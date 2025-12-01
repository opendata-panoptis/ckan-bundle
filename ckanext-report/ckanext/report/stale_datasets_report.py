'''
Stale datasets report - identifies datasets that haven't been updated according to their frequency.
'''

from ckan import model
from datetime import date, datetime
from ckan.plugins import toolkit

try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckanext.report import lib


# STRICT Frequency mapping - maps frequency values to EXACT time intervals in days
# Each frequency value corresponds EXACTLY to its literal meaning
# Supports both simple strings and EU authority URIs
# Based on the comprehensive vocabulary list with all 42 possible frequency values
FREQUENCY_MAP = {
    # Minute-based frequencies (EXACT minute intervals converted to days)
    "1MIN": 1/1440,     # Every minute = 1/1440 days (1 minute exactly)
    "5MIN": 5/1440,     # Every 5 minutes = 5/1440 days (5 minutes exactly)
    "10MIN": 10/1440,   # Every 10 minutes = 10/1440 days (10 minutes exactly)
    "15MIN": 15/1440,   # Every 15 minutes = 15/1440 days (15 minutes exactly)
    "30MIN": 30/1440,   # Every 30 minutes = 30/1440 days (30 minutes exactly)
    
    # Hour-based frequencies (EXACT hour intervals)
    "HOURLY": 1/24,     # Every hour = 1/24 days (1 hour exactly)
    "BIHOURLY": 2/24,   # Every 2 hours = 2/24 days (2 hours exactly)
    "TRIHOURLY": 3/24,  # Every 3 hours = 3/24 days (3 hours exactly)
    "12HRS": 12/24,     # Every 12 hours = 12/24 days (12 hours exactly)
    
    # Daily frequencies (EXACT day intervals)
    "DAILY": 1,         # Daily update = 1 day exactly
    "DAILY_2": 0.5,     # Twice daily = 0.5 days (12 hours exactly)
    
    # Weekly frequencies (EXACT weekly intervals)
    "WEEKLY": 7,        # Weekly update = 7 days exactly
    "WEEKLY_2": 7/2,    # Twice weekly = 7/2 days (3.5 days exactly)
    "WEEKLY_3": 7/3,    # Three times weekly = 7/3 days (2.33 days exactly)
    "WEEKLY_5": 7/5,    # Five times weekly = 7/5 days (1.4 days exactly)
    "BIWEEKLY": 14,     # Every two weeks = 14 days exactly
    
    # Monthly frequencies (EXACT monthly intervals)
    "MONTHLY": 30,      # Monthly update = 30 days exactly
    "MONTHLY_2": 15,    # Twice monthly = 15 days exactly (fortnightly)
    "MONTHLY_3": 10,    # Three times monthly = 10 days exactly
    "BIMONTHLY": 60,    # Every two months = 60 days exactly
    
    # Quarterly and annual frequencies (EXACT intervals)
    "QUARTERLY": 90,    # Quarterly = 90 days exactly (every 3 months)
    "ANNUAL": 365,      # Annual = 365 days exactly
    "ANNUAL_2": 182.5,  # Semi-annual = 365/2 days (182.5 days exactly)
    "ANNUAL_3": 365/3,  # Three times yearly = 365/3 days (121.67 days exactly)
    
    # Multi-year frequencies
    "BIENNIAL": 730,        # Every two years
    "TRIENNIAL": 1095,      # Every three years
    "QUADRENNIAL": 1460,    # Every four years
    "QUINQUENNIAL": 1825,   # Every five years
    "DECENNIAL": 3650,      # Every ten years
    "BIDECENNIAL": 7300,    # Every twenty years
    "TRIDECENNIAL": 10950,  # Every thirty years
    
    # Special frequencies - excluded from stale checking
    "AS_NEEDED": None,      # As needed - no fixed schedule
    "CONT": None,           # Continuous - always updating
    "IRREG": None,          # Irregular - no fixed pattern
    "NEVER": None,          # Never updated
    "NOT_PLANNED": None,    # Not planned for updates
    "OTHER": None,          # Other frequency not specified
    "UNKNOWN": None,        # Unknown frequency
    "UPDATE_CONT": None,    # Continuous update
    
    # Legacy mappings for backward compatibility
    "CONTINUOUS": None,     # Legacy mapping
    "IRREGULAR": None,      # Legacy mapping
    "REAL_TIME": 1,        # Legacy mapping
    "ONGOING": None,       # Legacy mapping
    "HALF_YEARLY": 182,    # Legacy mapping (same as ANNUAL_2)
    "EVERY_2_YEARS": 730,  # Legacy mapping (same as BIENNIAL)
    "EVERY_3_YEARS": 1095, # Legacy mapping (same as TRIENNIAL)
    "EVERY_5_YEARS": 1825, # Legacy mapping (same as QUINQUENNIAL)
    "EVERY_10_YEARS": 3650, # Legacy mapping (same as DECENNIAL)
    
    # EU Authority URI format - EXACT time intervals (matching simple format)
    "http://publications.europa.eu/resource/authority/frequency/1MIN": 1/1440,
    "http://publications.europa.eu/resource/authority/frequency/5MIN": 5/1440,
    "http://publications.europa.eu/resource/authority/frequency/10MIN": 10/1440,
    "http://publications.europa.eu/resource/authority/frequency/15MIN": 15/1440,
    "http://publications.europa.eu/resource/authority/frequency/30MIN": 30/1440,
    "http://publications.europa.eu/resource/authority/frequency/HOURLY": 1/24,
    "http://publications.europa.eu/resource/authority/frequency/BIHOURLY": 2/24,
    "http://publications.europa.eu/resource/authority/frequency/TRIHOURLY": 3/24,
    "http://publications.europa.eu/resource/authority/frequency/12HRS": 12/24,
    "http://publications.europa.eu/resource/authority/frequency/DAILY": 1,
    "http://publications.europa.eu/resource/authority/frequency/DAILY_2": 0.5,
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY": 7,
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY_2": 7/2,
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY_3": 7/3,
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY_5": 7/5,
    "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY": 14,
    "http://publications.europa.eu/resource/authority/frequency/MONTHLY": 30,
    "http://publications.europa.eu/resource/authority/frequency/MONTHLY_2": 15,
    "http://publications.europa.eu/resource/authority/frequency/MONTHLY_3": 10,
    "http://publications.europa.eu/resource/authority/frequency/BIMONTHLY": 60,
    "http://publications.europa.eu/resource/authority/frequency/QUARTERLY": 90,
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL": 365,
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2": 182.5,
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_3": 365/3,
    "http://publications.europa.eu/resource/authority/frequency/BIENNIAL": 730,
    "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL": 1095,
    "http://publications.europa.eu/resource/authority/frequency/QUADRENNIAL": 1460,
    "http://publications.europa.eu/resource/authority/frequency/QUINQUENNIAL": 1825,
    "http://publications.europa.eu/resource/authority/frequency/DECENNIAL": 3650,
    "http://publications.europa.eu/resource/authority/frequency/BIDECENNIAL": 7300,
    "http://publications.europa.eu/resource/authority/frequency/TRIDECENNIAL": 10950,
    "http://publications.europa.eu/resource/authority/frequency/AS_NEEDED": None,
    "http://publications.europa.eu/resource/authority/frequency/CONT": None,
    "http://publications.europa.eu/resource/authority/frequency/IRREG": None,
    "http://publications.europa.eu/resource/authority/frequency/NEVER": None,
    "http://publications.europa.eu/resource/authority/frequency/NOT_PLANNED": None,
    "http://publications.europa.eu/resource/authority/frequency/OTHER": None,
    "http://publications.europa.eu/resource/authority/frequency/UNKNOWN": None,
    "http://publications.europa.eu/resource/authority/frequency/UPDATE_CONT": None,
    
    # Legacy EU Authority URI mappings
    "http://publications.europa.eu/resource/authority/frequency/CONTINUOUS": None,
    "http://publications.europa.eu/resource/authority/frequency/IRREGULAR": None,
}


def normalize_frequency(frequency_value):
    """
    Normalize frequency value by handling empty strings and whitespace.
    Returns None for empty/invalid values.
    """
    if not frequency_value or not frequency_value.strip():
        return None
    return frequency_value.strip()


def get_frequency_display_name(frequency_value):
    """
    Get a user-friendly display name for frequency values.
    Converts frequency codes to translatable display names.
    """
    if not frequency_value:
        return toolkit._("Unknown")
    
    # Normalize input by trimming whitespace
    frequency_value = frequency_value.strip()
    
    if not frequency_value:
        return toolkit._("Unknown")
    
    # Handle EU authority URIs
    if frequency_value.startswith("http://publications.europa.eu/resource/authority/frequency/"):
        freq_code = frequency_value.split("/")[-1]
        frequency_value = freq_code
    
    # Map frequency codes to translatable display names
    frequency_translations = {
        # Minute-based frequencies
        "1MIN": toolkit._("Every minute"),
        "5MIN": toolkit._("Every 5 minutes"), 
        "10MIN": toolkit._("Every 10 minutes"),
        "15MIN": toolkit._("Every 15 minutes"),
        "30MIN": toolkit._("Every 30 minutes"),
        
        # Hour-based frequencies
        "HOURLY": toolkit._("Hourly"),
        "BIHOURLY": toolkit._("Every 2 hours"),
        "TRIHOURLY": toolkit._("Every 3 hours"),
        "12HRS": toolkit._("Every 12 hours"),
        
        # Daily frequencies
        "DAILY": toolkit._("Daily"),
        "DAILY_2": toolkit._("Twice daily"),
        
        # Weekly frequencies
        "WEEKLY": toolkit._("Weekly"),
        "WEEKLY_2": toolkit._("Twice weekly"),
        "WEEKLY_3": toolkit._("Three times weekly"),
        "WEEKLY_5": toolkit._("Five times weekly"),
        "BIWEEKLY": toolkit._("Every 2 weeks"),
        
        # Monthly frequencies
        "MONTHLY": toolkit._("Monthly"),
        "MONTHLY_2": toolkit._("Twice monthly"),
        "MONTHLY_3": toolkit._("Three times monthly"),
        "BIMONTHLY": toolkit._("Every 2 months"),
        
        # Quarterly and annual frequencies
        "QUARTERLY": toolkit._("Quarterly"),
        "ANNUAL": toolkit._("Annually"),
        "ANNUAL_2": toolkit._("Twice yearly"),
        "ANNUAL_3": toolkit._("Three times yearly"),
        
        # Multi-year frequencies
        "BIENNIAL": toolkit._("Every 2 years"),
        "TRIENNIAL": toolkit._("Every 3 years"),
        "QUADRENNIAL": toolkit._("Every 4 years"),
        "QUINQUENNIAL": toolkit._("Every 5 years"),
        "DECENNIAL": toolkit._("Every 10 years"),
        "BIDECENNIAL": toolkit._("Every 20 years"),
        "TRIDECENNIAL": toolkit._("Every 30 years"),
        
        # Special frequencies
        "AS_NEEDED": toolkit._("As needed"),
        "CONT": toolkit._("Continuous"),
        "IRREG": toolkit._("Irregular"),
        "NEVER": toolkit._("Never"),
        "NOT_PLANNED": toolkit._("Not planned"),
        "OTHER": toolkit._("Other"),
        "UNKNOWN": toolkit._("Unknown"),
        "UPDATE_CONT": toolkit._("Continuously updated"),
    }
    
    # Return translated name if available, otherwise format the raw value
    return frequency_translations.get(frequency_value, frequency_value.replace("_", " ").title())


def stale_datasets_report(organization=None):
    '''
    Produces a report on datasets that are stale based on their frequency.
    Returns something like this:
        {
         'table': [
            {'name': 'river-levels', 'title': 'River levels', 'organization': 'Environment Agency',
             'frequency': 'DAILY', 'last_modified': '2024-01-01', 'days_since_update': 45, 'status': 'STALE'},
            {'name': 'census-data', 'title': 'Census Data', 'organization': 'Statistics Office',
             'frequency': 'ANNUAL', 'last_modified': '2023-12-01', 'days_since_update': 90, 'status': 'OK'},
            ],
         'num_packages': 150,
         'num_stale': 25,
         'stale_percentage': 17,
        }
    '''
    
    # Query active datasets (excluding showcases)
    q = model.Session.query(model.Package) \
             .filter(model.Package.state == 'active')
    
    # Filter out showcases - exclude packages that are showcases
    q = lib.filter_datasets_only(q)
    
    if organization:
        q = lib.filter_by_organizations(q, organization, False)
    
    today = date.today()
    results = []
    total_checked = 0
    stale_count = 0
    
    for pkg in q:
        # Get frequency from extras
        raw_frequency = None
        if hasattr(pkg, 'extras') and pkg.extras:
            raw_frequency = pkg.extras.get('frequency')
        
        # Normalize frequency value
        frequency = normalize_frequency(raw_frequency)
        
        # ALWAYS include all datasets -
        # If no frequency, categorize as "M/Δ" (N/A)
        
        # Handle datasets without frequency or with unmapped frequency
        if not frequency or frequency not in FREQUENCY_MAP:
            # No frequency data - categorize as "M/Δ" 
            threshold_days = None
            status = toolkit._("N/A")
            days_since_update = None
            frequency_display = toolkit._("M/Δ")  # Greek for N/A
        else:
            # Get threshold days for this frequency
            threshold_days = FREQUENCY_MAP[frequency]
            frequency_display = get_frequency_display_name(frequency)
            
            # Handle frequencies that don't have a threshold (None values) 
            if threshold_days is None:
                status = toolkit._("N/A")
                days_since_update = None
            else:
                # Calculate days since last modification
                if isinstance(pkg.metadata_modified, datetime):
                    last_modified_date = pkg.metadata_modified.date()
                else:
                    last_modified_date = pkg.metadata_modified
                    
                days_since_update = (today - last_modified_date).days
                
                # Determine status based on threshold
                if days_since_update > threshold_days:
                    status = toolkit._("STALE")
                    stale_count += 1
                else:
                    status = toolkit._("OK")
        
        # Always count all datasets (including those without frequency)
        total_checked += 1
        
        # Get organization name
        org_name = ""
        if pkg.owner_org:
            try:
                org = model.Group.get(pkg.owner_org)
                if org:
                    org_name = org.display_name or org.name
            except:
                org_name = pkg.owner_org
        
        # Add to results
        # Ensure title and notes are never None to prevent template errors
        title = lib.resolve_dataset_title(pkg)
        notes = lib.dataset_notes(pkg) or ''
        
        results.append(OrderedDict([
            ('name', pkg.name),
            ('title', title),
            ('organization', org_name),
            ('frequency', frequency_display),
            ('frequency_raw', frequency),  # Keep raw value for debugging
            ('last_modified', str(last_modified_date) if 'last_modified_date' in locals() else toolkit._('N/A')),
            ('days_since_update', days_since_update),
            ('status', status),
            ('notes', notes),
        ]))
    
    # Calculate statistics
    stale_percentage = lib.percent(stale_count, total_checked) if total_checked > 0 else 0
    
    return {
        'table': results,
        'num_packages': total_checked,
        'num_stale': stale_count,
        'stale_percentage': stale_percentage,
        'total_datasets': q.count(),
    }


def stale_datasets_option_combinations():
    '''Generate option combinations for the report'''
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


# Report configuration
stale_datasets_report_info = {
    'name': 'stale-datasets',
    'title': toolkit._('Stale Datasets'),
    'description': toolkit._('Datasets that have not been updated according to their frequency schedule'),
    'option_defaults': OrderedDict((
        ('organization', None),
    )),
    'option_combinations': stale_datasets_option_combinations,
    'generate': stale_datasets_report,
    'template': 'report/stale-datasets.html',
}