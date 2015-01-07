#Create fake data
student.n <- 80000

student.data <- data.frame(
  #ID
  id = c(1:student.n),
  
  #Discrete data
  cat1 = rbinom(student.n, 1, 0.5),
  cat2 = rbinom(student.n, 1, 0.5),
  cat3 = rbinom(student.n, 1, 0.5),
  cat4 = rbinom(student.n, 1, 0.5),
  
  #No-match group
  nomatch = rep("", student.n),
  
  #Continuous data
  cont1 = rnorm(student.n),
  cont2 = rnorm(student.n),
  cont3 = rnorm(student.n),
  cont4 = rnorm(student.n)
  )

#Create categories groups
student.data$group <- paste(student.data$cat1, 
                            student.data$cat2, 
                            student.data$cat3, 
                            student.data$cat4, sep="")

#Delete individual groups
student.data$cat1 <- NULL
student.data$cat2 <- NULL
student.data$cat3 <- NULL
student.data$cat4 <- NULL

#Rearrange
student.data <- student.data[c("id",
                               "group",
                               "nomatch",
                               "cont1",
                               "cont2",
                               "cont3",
                               "cont4")]

#Write as CSV
write.table(student.data, file="student.data.csv", row.names=FALSE, col.names=FALSE, sep=",")
