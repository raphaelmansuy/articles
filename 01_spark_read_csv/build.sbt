import sbt._
import java.io.File
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.10"


lazy val root = (project in file("."))
  .settings(
    name := "SparkReadCSV"
  )

lazy val global = project
  .in(file("."))

libraryDependencies ++= Dependencies.commonDependencies
