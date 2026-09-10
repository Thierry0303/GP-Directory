#!/usr/bin/env python3
"""
Shared London geography helpers: map an outward postcode district to a London
borough, and expose the set of London districts. Used by the pharmacy pipeline
(fetch_pharmacies.py, build_pharmacy_pages.py) to define "London" the same way
the rest of the site does.
"""
import re

# Outward postcode district -> London borough. Mirrors the mapping the GP
# pipeline uses (merge_into_dataset.py / refresh_nhs_data.py).
BOROUGH_MAP = {
    'E1':'Tower Hamlets','E2':'Tower Hamlets','E3':'Tower Hamlets',
    'E4':'Waltham Forest','E5':'Hackney','E6':'Newham','E7':'Newham',
    'E8':'Hackney','E9':'Hackney','E10':'Waltham Forest',
    'E11':'Waltham Forest','E12':'Newham','E13':'Newham',
    'E14':'Tower Hamlets','E15':'Newham','E16':'Newham',
    'E17':'Waltham Forest','E18':'Redbridge','E20':'Newham',
    'EC1':'Islington','EC2':'City of London','EC3':'City of London','EC4':'City of London',
    'N1':'Islington','N2':'Barnet','N3':'Barnet','N4':'Haringey',
    'N5':'Islington','N6':'Haringey','N7':'Islington','N8':'Haringey',
    'N9':'Enfield','N10':'Haringey','N11':'Barnet','N12':'Barnet',
    'N13':'Enfield','N14':'Enfield','N15':'Haringey','N16':'Hackney',
    'N17':'Haringey','N18':'Enfield','N19':'Islington','N20':'Barnet',
    'N21':'Enfield','N22':'Haringey',
    'NW1':'Camden','NW2':'Brent','NW3':'Camden','NW4':'Barnet',
    'NW5':'Camden','NW6':'Brent','NW7':'Barnet','NW8':'Westminster',
    'NW9':'Brent','NW10':'Brent','NW11':'Barnet',
    'SE1':'Southwark','SE2':'Greenwich','SE3':'Greenwich','SE4':'Lewisham',
    'SE5':'Southwark','SE6':'Lewisham','SE7':'Greenwich','SE8':'Lewisham',
    'SE9':'Greenwich','SE10':'Greenwich','SE11':'Lambeth','SE12':'Lewisham',
    'SE13':'Lewisham','SE14':'Lewisham','SE15':'Southwark','SE16':'Southwark',
    'SE17':'Southwark','SE18':'Greenwich','SE19':'Bromley','SE20':'Bromley',
    'SE21':'Southwark','SE22':'Southwark','SE23':'Lewisham','SE24':'Lambeth',
    'SE25':'Croydon','SE26':'Lewisham','SE27':'Lambeth','SE28':'Greenwich',
    'SW1':'Westminster','SW2':'Lambeth','SW3':'Kensington & Chelsea',
    'SW4':'Lambeth','SW5':'Kensington & Chelsea','SW6':'Hammersmith & Fulham',
    'SW7':'Kensington & Chelsea','SW8':'Lambeth','SW9':'Lambeth',
    'SW10':'Kensington & Chelsea','SW11':'Wandsworth','SW12':'Wandsworth',
    'SW13':'Richmond','SW14':'Richmond','SW15':'Wandsworth','SW16':'Lambeth',
    'SW17':'Wandsworth','SW18':'Wandsworth','SW19':'Merton','SW20':'Merton',
    'W1':'Westminster','W2':'Westminster','W3':'Ealing','W4':'Hounslow',
    'W5':'Ealing','W6':'Hammersmith & Fulham','W7':'Ealing',
    'W8':'Kensington & Chelsea','W9':'Westminster','W10':'Kensington & Chelsea',
    'W11':'Kensington & Chelsea','W12':'Hammersmith & Fulham',
    'W13':'Ealing','W14':'Hammersmith & Fulham',
    'WC1':'Camden','WC2':'Westminster',
    'BR1':'Bromley','BR2':'Bromley','BR3':'Bromley','BR4':'Bromley',
    'BR5':'Bromley','BR6':'Bromley','BR7':'Bromley',
    'CR0':'Croydon','CR2':'Croydon','CR4':'Merton','CR5':'Croydon',
    'CR7':'Croydon','CR8':'Croydon',
    'DA1':'Bexley','DA5':'Bexley','DA6':'Bexley','DA7':'Bexley',
    'DA8':'Bexley','DA14':'Bexley','DA15':'Bexley','DA16':'Bexley','DA17':'Bexley',
    'EN1':'Enfield','EN2':'Enfield','EN3':'Enfield','EN4':'Barnet',
    'EN5':'Barnet','EN8':'Enfield','EN9':'Enfield',
    'HA0':'Brent','HA1':'Harrow','HA2':'Harrow','HA3':'Harrow',
    'HA4':'Hillingdon','HA5':'Harrow','HA6':'Hillingdon',
    'HA7':'Harrow','HA8':'Barnet','HA9':'Brent',
    'IG1':'Redbridge','IG2':'Redbridge','IG3':'Redbridge','IG4':'Redbridge',
    'IG5':'Redbridge','IG6':'Redbridge','IG7':'Redbridge','IG8':'Redbridge',
    'IG11':'Barking & Dagenham',
    'KT1':'Kingston','KT2':'Kingston','KT3':'Kingston','KT4':'Kingston',
    'KT5':'Kingston','KT6':'Kingston','KT7':'Kingston','KT8':'Richmond','KT9':'Kingston',
    'RM1':'Havering','RM2':'Havering','RM3':'Havering','RM5':'Havering',
    'RM6':'Barking & Dagenham','RM7':'Havering','RM8':'Barking & Dagenham',
    'RM9':'Barking & Dagenham','RM10':'Barking & Dagenham',
    'RM11':'Havering','RM12':'Havering','RM13':'Havering','RM14':'Havering',
    'SM1':'Sutton','SM2':'Sutton','SM3':'Sutton','SM4':'Merton',
    'SM5':'Sutton','SM6':'Sutton',
    'TW1':'Richmond','TW2':'Richmond','TW3':'Hounslow','TW4':'Hounslow',
    'TW5':'Hounslow','TW6':'Hounslow','TW7':'Hounslow','TW8':'Hounslow',
    'TW9':'Richmond','TW10':'Richmond','TW11':'Richmond','TW12':'Richmond',
    'TW13':'Hounslow','TW14':'Hounslow',
    'UB1':'Ealing','UB2':'Ealing','UB3':'Hillingdon','UB4':'Hillingdon',
    'UB5':'Ealing','UB6':'Ealing','UB7':'Hillingdon','UB8':'Hillingdon',
    'UB9':'Hillingdon','UB10':'Hillingdon',
}


def outward_district(postcode: str) -> str:
    """Return the outward code (e.g. 'N20' from 'N20 0DH')."""
    if not postcode:
        return ""
    pc = postcode.strip().upper()
    if " " in pc:
        return pc.split()[0]
    m = re.match(r'([A-Z]{1,2}\d[A-Z\d]?)', pc)
    return m.group(1) if m else pc


def borough_from_postcode(postcode: str) -> str:
    """Map a postcode to a London borough, or '' if not in London."""
    d = outward_district(postcode)
    if d in BOROUGH_MAP:
        return BOROUGH_MAP[d]
    # Fall back to the letters+first-digit district (e.g. 'SW1A' -> 'SW1').
    m = re.match(r'([A-Z]{1,2}\d)', d)
    return BOROUGH_MAP.get(m.group(1), "") if m else ""


def is_london(postcode: str) -> bool:
    return bool(borough_from_postcode(postcode))
