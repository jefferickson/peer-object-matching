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
  
  #Continuous data
  cont1 = rnorm(student.n),
  cont2 = rnorm(student.n),
  cont3 = rnorm(student.n),
  cont4 = rnorm(student.n)
  )

#Write as CSV
write.table(student.data, file="student.data.csv", row.names=FALSE, col.names=FALSE, sep=",")