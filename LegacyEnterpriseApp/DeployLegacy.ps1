#Do precompiled steps here and publish the Webapp as described here https://docs.microsoft.com/en-us/aspnet/mvc/overview/deployment/docker-aspnetmvc#publish-script
#Copy Dockerfile2 to /bin/Release/PublishOutput and rename to Dockerfile. Then do the following docker commands changing the name of the repo and tag version
docker build -t legacyenterprisewebapp .
docker tag legacyenterprisewebapp:latest mfussell/legacyenterprisewebapp:v3
docker push mfussell/legacyenterprisewebapp:v3

#Do precompiled steps here and publish the Dataapp as described here https://docs.microsoft.com/en-us/aspnet/mvc/overview/deployment/docker-aspnetmvc#publish-script
#Copy Dockerfile2 to /bin/Release/PublishOutput and rename to Dockerfile. Then do the following docker commands changing the name of the repo and tag version
docker build -t legacyenterprisedataapp .
docker tag legacyenterprisedataapp:latest mfussell/legacyenterprisedataapp:v3
docker push mfussell/legacyenterprisedataapp:v3

#use this to test and run locally
docker run -d --name testdata mfussell/legacyenterprisedataapp:v3
docker run -d --name testweb  mfussell/legacyenterprisewebapp:v3
docker inspect -f "{{ .NetworkSettings.Networks.nat.IPAddress }}" testweb
#get IP address returned from inspect command and type this into a browser. You should see the app run locally.

# use this stop and remove local containers
docker rm testweb -f
docker rm testdata -f

#Use this to remove old local images 
docker rmi mfussell/legacyenterprisedataapp:v1
docker rmi mfussell/legacyenterprisewebapp:v1

#To run images in Service Fabric from container registry that you pushed to.
Connect-ServiceFabricCluster [name ofcluster]
New-ServiceFabricComposeApplication -ApplicationName "fabric:/legacyapp" -Compose "Cdocker-compose-prebuilt.yml"
Remove-ServiceFabricComposeApplication -ApplicationName "fabric:/legacyapp" 

