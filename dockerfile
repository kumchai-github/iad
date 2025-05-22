#FROM maven:3.9.9-eclipse-temurin-21-alpine AS build
FROM harbordev.se.scb.co.th/library/maven:3.9.9-eclipse-temurin-21-alpine AS build

# Set working directory
WORKDIR /app

# Copy your Maven project files
COPY pom.xml .
COPY src ./src

# Package the application (adjust to your goal)
#RUN mvn clean package -DskipTests
#RUN mvn -B --no-transfer-progress clean package -DskipTests
RUN mvn clean package
RUN ls -la /app/target/