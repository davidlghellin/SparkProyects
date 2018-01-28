package es.david.titanic.model

case class Pasajeros(passengerId: Integer, survived: Integer, pclass: Integer, name: String, sex: String,
                     age: Float, sibSp: Integer, parch: Integer, ticket: String, fare: Float, cabin: String, embarked: String)