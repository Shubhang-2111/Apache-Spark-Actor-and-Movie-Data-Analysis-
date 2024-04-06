import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MoviesAndActors extends App {

  case class MovieRecords(movie_id:Int,character:String)
  case class Movies(movie_id:Int,title:String,genre:String,release_year:String,director:String)
  case class Actors(actor_id:Int,name:String,age:Int,moviesRecords:Seq[MovieRecords])

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setMaster("local[*]").setAppName("MoviesAndActors")

  val sc = new SparkContext(conf)

  val actorPath = "E:\\JsonParsing\\data\\actors.json"
  val moviePath = "E:\\JsonParsing\\data\\movies.json"

  val movieRdd = sc.textFile(moviePath)
  val actorRdd = sc.textFile(actorPath)

  val movieJsonRdd = movieRdd.map(parse(_))
  val actorJsonRdd = actorRdd.map(parse(_))

  val movieInfoRdd = movieJsonRdd.map(value => {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val movie_id = (value \ "movie_id").extract[Int]
    val title = (value \ "title").extract[String]
    val genre = (value \ "genre").extract[String]
    val release_year = (value \ "release_year").extract[String]
    val director = (value \ "director").extract[String]
    Movies(movie_id,title,genre,release_year,director)
  })

  val actorsInfoRdd = actorJsonRdd.map(value =>{
    implicit val formats: DefaultFormats.type = DefaultFormats
    val actor_id = (value \ "actor_id").extract[Int]
    val name  = (value\ "name").extract[String]
    val age = (value \ "age").extract[Int]
    val moviesJson = value \ "movies"
    val movies = moviesJson.children.map { movie =>
      val movie_id = (movie \ "movie_id").extract[Int]
      val character = (movie \ "character").extract[String]
      MovieRecords(movie_id, character)
    }
    Actors(actor_id,name,age,movies)
  })

 // Movies details for particular Id

  def movieDetails(movieId:Int): Unit ={
    val requiredMovie = movieInfoRdd.filter(_.movie_id==movieId)

    requiredMovie.foreach{movie =>
      println("Movies name ->" + movie.title)
      println("Movie Genre ->" + movie.genre)
    }
  }


  // Calculate average age of actors

  def averageAgeOfActors(): Unit = {
    val ageAndNumbers = actorsInfoRdd.map(el => (1, (1, el.age)))
    val avgAge = ageAndNumbers.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val age = avgAge.map(el => el).first

    println("Average age of actors is " + (age._2._2) / (age._2._1))
  }

  // Number of Actors in a movie

  def findNumberOfActorInMovie(): Unit ={
    val movieFilmCount = actorsInfoRdd.flatMap(_.moviesRecords.map(_.movie_id))
      .map(movieId => (movieId,1))
      .reduceByKey((x,y) => (x+y))

    movieFilmCount.foreach(el => println(s"${el._1} movie Id : Number of Actors ${el._2}"))
  }

  // List of all movies of particular genre

  def allMoviesWithGenre(genre:String): Unit = {

    val allMovies: RDD[Movies] = movieInfoRdd.filter(_.genre == genre)

    println("GENRE -> "+genre)
    allMovies.foreach(el => println("Movie Name -> "+el.title))

  }

  // Sort movies name by their release year
  def sortMoviesByReleaseYear():Unit={
    val movies = movieInfoRdd.map(movie => (movie.release_year,movie.title))

    val sortedMovies = movies.sortBy(_._1)

    sortedMovies.collect().foreach(movies => println("Release Year : "+movies._1+" Name -> "+movies._2))

  }

  // Count the number of movies released each year

  def moviesEachYear():Unit={
    val moviesPerYear = movieInfoRdd.map(el => (el.release_year,el.title))
    val listOfMoviesEachYear = moviesPerYear.reduceByKey(_++_)

    listOfMoviesEachYear.foreach((el) => println("Movies in Release Year "+el._1 +" : ",el._2))
  }

  //Determine the most common genres in the movies data

  def mostCommonGenre():Unit={
    val allGenre = movieInfoRdd.map(movies => (movies.genre,1))
    val numberOfMoviesPerGenre = allGenre.reduceByKey(_+_)
    val mostCommonGenre = numberOfMoviesPerGenre.values.max()
    val listMostCommonGenre = numberOfMoviesPerGenre
      .flatMap(el => if(el._2==mostCommonGenre) List(el._1) else List())

    println(mostCommonGenre)
    listMostCommonGenre.foreach(println)
  }

  // function to find all the movies by an actor of id _
  // JOINS

  def findMoviesByActor(id: Int): Unit = {

    val detailsOfActor = actorsInfoRdd.filter(_.actor_id == id)

    val listOfMoviesId: RDD[(Int, String)] = detailsOfActor
      .flatMap(actor => actor.moviesRecords
        .map(movie => (movie.movie_id, actor.name)))

    val detailsOfMovies: RDD[(Int, String)] = movieInfoRdd.map(movie => (movie.movie_id, movie.title))

    val allMovieNameWithActor = listOfMoviesId.join(detailsOfMovies)

    allMovieNameWithActor.foreach(actor => println("Actor Name : "+actor._2._1+" -- Movies -"+actor._2._2))

  }

  // List all actors and the movies they've appeared in along with their character roles.
  // JOINS

  def allActorsInMoviesWithCharacters():Unit={

    val joinStartTime = System.nanoTime()

    val allMoviesIdOfActors: RDD[(Int, (String, String))] = actorsInfoRdd.flatMap(el => (el.moviesRecords.map(movies => (movies.movie_id,(el.name,movies.character)))))
    val allMoviesName: RDD[(Int, String)] = movieInfoRdd.map(movies => (movies.movie_id,movies.title))

    val joinedMoviesAndActors: RDD[(Int, (String, (String, String)))] = allMoviesName.join(allMoviesIdOfActors)

    val mappedActorsToMovies = joinedMoviesAndActors.map(movie => (movie._2._2._1,List((movie._2._1,movie._2._2._2))))

    val reducedActorsToMovies: RDD[(String, List[(String, String)])] =  mappedActorsToMovies.reduceByKey(_++_)

    reducedActorsToMovies.collect().foreach{actor =>
      print("ACTOR NAME : " + actor._1)
      println("     *** LIST OF MOVIES APPEARED IN ***        ")
      println("   --------------- MOVIE_NAME -------------- " + " ---------------CHARACTER --------------" )
      actor._2.foreach(el => println(" --- "+el._1 +"     ----  "+el._2))
    }

    val joinEndTime = System.nanoTime()
    println(s"Normal join time: ${(joinEndTime - joinStartTime) / 1e9} seconds")
  }

  // Above query using Broadcast Join

  def allActorsInMoviesWithCharactersBroadCast(): Unit = {

    val broadcastJoinStartTime = System.nanoTime()

    val allMoviesIdOfActors = actorsInfoRdd.flatMap(el => (el.moviesRecords
      .map(movies => (movies.movie_id, List((el.name, movies.character))))))

    val allMoviesName = movieInfoRdd.map(movies => (movies.movie_id, movies.title))

    val bcAllMoviesName: Broadcast[collection.Map[Int, String]] = sc.broadcast(allMoviesName.collectAsMap())

    val joinedMoviesAndActors: RDD[(String, List[(String, String)])] =  allMoviesIdOfActors
      .map(movie => (bcAllMoviesName.value.get(movie._1).toString,movie._2))

    val actorsToMovies = joinedMoviesAndActors
      .flatMap(el => el._2
        .map(actor => (actor._1,List((el._1,actor._2)))))

    val reducedActorsToMovies = actorsToMovies.reduceByKey(_++_)


    reducedActorsToMovies.collect().foreach { actor =>
      print("ACTOR NAME : " + actor._1)
      println("     *** LIST OF MOVIES APPEARED IN ***        ")
      println("   --------------- MOVIE_NAME -------------- " + " ---------------CHARACTER --------------")
      actor._2.foreach(el => println(" --- " + el._1 + "     ----  " + el._2))
    }

    val broadcastJoinEndTime = System.nanoTime()
    println(s"Broadcast join time: ${(broadcastJoinEndTime - broadcastJoinStartTime) / 1e9} seconds")

  }

  allActorsInMoviesWithCharactersBroadCast()

}
