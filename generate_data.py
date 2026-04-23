"""
generate_data.py  —  creates 3 intentionally messy CSV files in data/raw/
"""
import pandas as pd
import numpy as np
import os

np.random.seed(42)

STATES = [
    "Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh",
    "Goa","Gujarat","Haryana","Himachal Pradesh","Jharkhand",
    "Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur",
    "Meghalaya","Mizoram","Nagaland","Odisha","Punjab",
    "Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura",
    "Uttar Pradesh","Uttarakhand","West Bengal",
    "Andaman and Nicobar Islands","Chandigarh","Delhi",
]

ALIASES = {
    "Uttar Pradesh":["UP","U.P.","Uttar Pradesh","uttar pradesh"],
    "Tamil Nadu":["TN","T.N.","Tamil Nadu","tamil nadu"],
    "West Bengal":["WB","West Bengal","west bengal"],
    "Maharashtra":["MH","Maharashtra","MAHARASHTRA"],
    "Karnataka":["KA","Karnataka","KARNATAKA"],
    "Rajasthan":["RJ","Rajasthan","RAJASTHAN"],
    "Gujarat":["GJ","Gujarat","GUJARAT"],
    "Delhi":["DL","Delhi","New Delhi","NCT Delhi"],
    "Bihar":["BR","Bihar","BIHAR"],
    "Odisha":["OR","Odisha","Orissa"],
}

def ms(s):
    return np.random.choice(ALIASES[s]) if s in ALIASES else s

def gen_pop():
    rows=[]
    for s in STATES:
        p=np.random.randint(500000,230000000)
        m=int(p*np.random.uniform(0.48,0.52))
        d=round(np.random.uniform(10,11000),1)
        
        # Inject intentional nulls
        if np.random.rand()<0.10: d=None
            
        rows.append({
            "State_Name": ms(s),
            "total_population": p,
            "male_population": m,
            "female_population": p-m,
            "area_sq_km": np.random.randint(100,342000),
            "population_density": d,
            "urban_population_pct": round(np.random.uniform(10.0, 85.0), 2),
            "year": np.random.choice([2011,2021])
        })
    # Inject duplicate row
    rows.append(rows[2].copy())
    pd.DataFrame(rows).to_csv("data/raw/population.csv",index=False)
    print(f"  population.csv  — {len(rows)} rows")

def gen_lit():
    rows=[]
    for s in STATES:
        tot=round(np.random.uniform(55.0,96.0),2)
        mal=round(tot+np.random.uniform(2.0,12.0),2)
        fem=round(max(tot-np.random.uniform(2.0,15.0),30.0),2)
        
        # Inject intentional nulls
        if np.random.rand()<0.12: fem=None
        if np.random.rand()<0.08: mal=None
            
        rows.append({
            "state": ms(s),
            "literacy_rate_overall": tot,
            "literacy_rate_male": mal,
            "literacy_rate_female": fem,
            "primary_schools_per_1000": round(np.random.uniform(1.0, 10.0), 2),
            "higher_education_institutions": np.random.randint(10, 500),
            "govt_schools_pct": round(np.random.uniform(40.0, 95.0), 2),
            "survey_year": np.random.choice(["2011","2021","2011.0"])
        })
    # Inject entirely null row
    rows.append({k:None for k in rows[0].keys()})
    pd.DataFrame(rows).to_csv("data/raw/literacy.csv",index=False)
    print(f"  literacy.csv    — {len(rows)} rows")

def gen_emp():
    rows=[]
    for s in STATES:
        ag=round(np.random.uniform(15.0,75.0),2)
        ind=round(np.random.uniform(5.0,35.0),2)
        svc=round(100-ag-ind,2)
        un=round(np.random.uniform(1.0,12.0),2)
        
        # Inject intentional nulls
        if np.random.rand()<0.15: un=None
            
        rows.append({
            "STATE": ms(s),
            "labour_force_participation_pct": round(np.random.uniform(30.0,55.0),2),
            "agriculture_employment_pct": ag,
            "manufacturing_employment_pct": ind,
            "services_employment_pct": svc,
            "unemployment_rate_pct": un,
            "per_capita_income_inr": np.random.randint(45000, 350000),
            "gdp_contribution_pct": round(np.random.uniform(1.0, 15.0), 2),
            "year": np.random.choice([2011,2021,2022])
        })
    pd.DataFrame(rows).to_csv("data/raw/employment.csv",index=False)
    print(f"  employment.csv  — {len(rows)} rows")

if __name__=="__main__":
    os.makedirs("data/raw",exist_ok=True)
    print("Generating datasets...")
    gen_pop(); gen_lit(); gen_emp()
    print("Done.")