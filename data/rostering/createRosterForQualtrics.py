import pandas as pd

# TODO: Split this into subfunctions if necessary
# TODO: Create additional unit tests for robustness

#A function that takes in a csv of student names, emails, team names, and returns team/teammatenumbers/teammate names for Qualtrics
def createRosterForQualtrics(csvFileStr, firstNameCol, lastNameCol, teamNameCol, hasTeamNumbers, outputFileName):
    
    #importing and sorting data
    raw = pd.read_csv(csvFileStr)
    raw["FullName"] = raw[firstNameCol] + ' ' + raw[lastNameCol]
    raw["FullNameUpper"] = raw["FullName"].str.upper()
    raw = raw.sort_values(by=[teamNameCol, "FullNameUpper"], ascending=True)
    teams = raw[teamNameCol].unique()
    
    #instantiate new columns to add
    teammatenumbers = []
    teamnumbers = []
    team_sizes = []
    
    #figure out max team size
    for team in teams:
        subset_team = raw[raw[teamNameCol] == team]
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
     
    #fill in teamnumber column
    if not hasTeamNumbers:
        for section in sections:
            subset_section = raw[raw["Section"] == section].reset_index()
            curr_team = ''
            counter = 0
            for i in range(len(subset_section)):
                if subset_section.iloc[i]["TeamName"] == curr_team:
                    teamnumbers.append(section + '_Team_' + str(counter))
                else:
                    counter = counter + 1
                    curr_team = subset_section.iloc[i]["TeamName"]
                    teamnumbers.append(section + '_Team_' + str(counter))
        raw["TeamNumber"] = teamnumbers
    
    #number columns set
    raw["TeammateNumber"] = teammatenumbers
    raw = raw.reset_index(drop=True)
    
    #fill in teammateX names
    for team in teams:
        subset_team = raw[raw[teamNameCol] == team]
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
        
    raw = raw.drop('FullNameUpper', axis=1)
        
    return raw.to_csv(outputFileName)
