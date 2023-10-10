import pandas as pd
import numpy as np
import re

def createQualtricsRoster(csv_file_str):
    
    #importing and sorting data
    raw = pd.read_csv(csv_file_str)
    raw["TeamNumber"] = [int(i) for i in raw["TeamNumber"]]
    raw = raw.sort_values(by=["TeamNumber", "Last Name"], ascending=True).reset_index(drop=True)
    raw["FullName"] = raw["First Name"] + ' ' + raw["Last Name"]
    teams = raw["TeamNumber"].unique()
    #sections = raw["Section"].unique() #can remove if needed
    
    #instantiate new columns to add
    teammatenumbers = []
    #teamnumbers = [] not needed
    team_sizes = []
    
    #figure out max team size
    for team in teams:
        subset_team = raw[raw["TeamNumber"] == team]
        team_sizes.append(len(subset_team))
    max_team_size = max(team_sizes)
    
    #dynamically instantiate teammate columns
    teammate_cols = {}
    for i in range(max_team_size):
        teammate_str = "Teammate" + str(i + 1)
        teammate_cols[teammate_str] = []
    
    #fill in teammatenumber columnn
    for team_size in team_sizes:
        team_range = range(1, team_size + 1)
        for stu in team_range:
            teammatenumbers.append(stu)
    #print(sections)
    
    #number columns set
    #raw["TeamNumber"] = teamnumbers
    raw["TeammateNumber"] = teammatenumbers
    raw = raw.reset_index(drop=True)
    
    #fill in teammateX names
    for team in teams:
        subset_team = raw[raw["TeamNumber"] == team]
        team_size = len(subset_team)
        counter = 1
        for teammate_col in list(teammate_cols.keys()):
            curr_stu = subset_team[subset_team["TeammateNumber"] == counter]
            for i in range(team_size):
                name_lst = curr_stu["FullName"].tolist()
                if len(name_lst) == 0:
                    teammate_cols[teammate_col].append('')
                else:
                    teammate_cols[teammate_col].append(name_lst[0])
            counter += 1
    
    for key in list(teammate_cols.keys()):
        raw[key] = teammate_cols[key]
        
    return raw.to_csv("Cleaned_Roster_Output.csv")