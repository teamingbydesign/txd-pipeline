# README

This folder `data` contains sample data of previous semesters. Data within this folder is confidential and should not be shared without explicit written consent by TxD. 

The contents of this folder are the following:

**Files** 

1. Master_Question_Dictionary.csv
    - This csv file contains mappings between survey questions and their unique IDs. This dictionary was last edited for the Spring 2023 semester and has been active since the Fall 2022 semester
2. NUS_RAW.csv
    - This csv file contains raw survey data administered in an NUS class during CY2023. Sensitive information relating to students' location, etc. have been removed. Note that the last 2 columns of this file are not 'raw' since they were calculated using functions. Additional context relating to function use are explained below.
    

**Folders** 
Folders are ordered by number, which designate position in the overarching data processing workflow. This means that for each survey administered, the data pipeline contains 4 phases of processing: roster creation (1), data cleaning (2), student report creation (3), and faculty report creation (4).

Phase 1: createRoster
Phase 2: cleanData
Phase 3: generateStudentData
Phase 4: generateFacultyData




