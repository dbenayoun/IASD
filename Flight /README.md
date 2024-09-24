# Flight

1. check for possible wrong data 
2. treat missing values
3. focus on delayed flights only, filtered out diverted and canceled flights
4. remove weather not related to airport locations 

5. create the join table : create a tuple, {array(flight info(origin airport, destination,etc)), array(weather obesrvations at origin airport from the scheduled departure time back 12hours every hour), array(weather obesrvations at destination airport from the scheduled arrival time back 12hours every hour), class(on time or delayeda according to a threshold)}
Read the white paper for deails about the improved repartition joined 

6. target: we must isolate flights that had delays because of extreme weather 
7. we must bal;ance the dataset witn 50% on time and 50% delayed flights 

![reshuffle datset](2024-09-24_21-31-13.png)

8. create a feature with delays < 15min, between 15 and 60min, and above 60min => tran, test
9. Random forest : 

Input:

![input of the random forest](2024-09-24_21-31-03.png)

10. play witht the delay thresholds etc

