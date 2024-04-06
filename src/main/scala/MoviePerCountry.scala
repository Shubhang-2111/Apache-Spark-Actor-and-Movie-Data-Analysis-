import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object MoviePerCountry extends App{

  case class Details(countries: String, languages: String)

  case class Movie(movie_name: String, genre: String, actor: String, rating: Double, details: Details)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setMaster("local[*]").setAppName("parseMovies")

  val sc = new SparkContext(conf)

  val path = "E:\\JsonParsing\\data\\fake_movies_data.json"

  val rdd: RDD[String] = sc.textFile(path)

  val jsonRdd: RDD[JValue] = rdd.map(parse(_))


  val movieInfoRdd: RDD[Movie] = jsonRdd.map(value => {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val movieName = (value \ "movie_name").extract[String]
    val genre = (value \ "genre").extract[String]
    val actor = (value \ "actor").extract[String]
    val rating = (value \ "rating").extract[Double]
    val details = Details((value \ "details" \ "countries").extract[String], (value \ "details" \ "languages").extract[String])
    Movie(movieName, genre, actor, rating, details)
  })

//  movieInfoRdd.foreach(println)

  // Function to filter movies by country
  def filterMoviesByCountry(country: String): Unit = {
    val moviesInCountry = movieInfoRdd.filter(_.details.countries == country)
    println(s"Movies in $country:")
    moviesInCountry.foreach(movie => println(s"${movie.movie_name} (${movie.rating})"))
  }

  def aboveGivenRating(expectedRating:Int):Unit={
    val moviesAboveRating = movieInfoRdd.filter(_.rating > 3)

    println(s"Movies above $moviesAboveRating ")
    moviesAboveRating.foreach(movie => println(s"movie ${movie.movie_name} and  rating ${movie.rating}"))
  }


  aboveGivenRating(3)
//    Show movies in "French Southern Territories"
  filterMoviesByCountry("French Southern Territories")

//   Show movies in "Sierra Leone"
  filterMoviesByCountry("Sierra Leone")


}
