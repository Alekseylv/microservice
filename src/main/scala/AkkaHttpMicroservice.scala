import akka.event.{LoggingAdapter, Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{RequestEntity, HttpResponse, HttpRequest}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.marshalling.Marshal
import akka.stream.{ActorFlowMaterializer, FlowMaterializer}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.IOException
import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}
import spray.json.DefaultJsonProtocol
import akka.agent._
import akka.actor._

case class Order(user: User, product: String, deliveryAddress: String, price: Double)

case class User(firstName: String, lastName: String, uuid: String)

case class OrderRequest(user: UserRequest, product: String, deliveryAddress: String, price: Double)

case class UserRequest(firstName: String, lastName: String)

case class OrderInner(product: String, deliveryAddress: String, price: Double, uuid: String)

object Order {
  def apply(order: OrderInner, user: User): Order = Order(user, order.product, order.deliveryAddress, order.price)
}

object OrderInner {
  def apply(order: OrderRequest, uuid: String): OrderInner = OrderInner(order.product, order.deliveryAddress, order.price, uuid)
}

object User {
  def apply(user: UserRequest, uuid: String): User = User(user.firstName, user.lastName, uuid)
}

trait Protocols extends DefaultJsonProtocol {
  implicit val userFormat = jsonFormat3(User.apply)
  implicit val orderFormat = jsonFormat4(Order.apply)
  implicit val userRequestFormat = jsonFormat2(UserRequest.apply)
  implicit val orderRequestFormat = jsonFormat4(OrderRequest.apply)
}

trait Service extends Protocols {

  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: FlowMaterializer

  def config: Config

  val logger: LoggingAdapter

  lazy val orders = Agent[Map[String, OrderInner]](Map.empty)
  lazy val users = Agent[Map[String, User]](Map.empty)


  lazy val userConnectionFlow: Flow[HttpRequest, HttpResponse, Any] =
    Http().outgoingConnection(config.getString("services.userHost"), config.getInt("services.userPort"))

  def userRequest(request: HttpRequest): Future[HttpResponse] = Source.single(request).via(userConnectionFlow).runWith(Sink.head)

  def createOrder(order: OrderRequest): Future[String] = {
    Marshal(order.user).to[RequestEntity].flatMap(x =>
      userRequest(RequestBuilding.Post("/user").withEntity(x))).flatMap {
      response =>
        response.status match {
          case OK =>
            val uuid = Unmarshal(response.entity).to[String]
            uuid.flatMap(x => orders.alter(_.updated(x, OrderInner(order, x)))).flatMap(x => uuid)
          case _ => Unmarshal(response.entity).to[String].flatMap {
            entity =>
              val error = s"User create request failed with status code ${response.status} and entity $entity"
              logger.error(error)
              Future.failed(new IOException(error))
          }
        }
    }
  }

  def fetchOrder(uuid: String): Option[Future[Order]] = {
    orders.get().get(uuid).map {
      order =>
        userRequest(RequestBuilding.Get(s"/user/$uuid")).flatMap {
          response =>
            response.status match {
              case OK => Unmarshal(response.entity).to[User].map(Order(order, _))
              case _ => Unmarshal(response.entity).to[String].flatMap {
                entity =>
                  val error = s"User request failed with status code ${response.status} and entity $entity"
                  logger.error(error)
                  Future.failed(new IOException(error))
              }
            }
        }
    }
  }

  def fetchUser(uuid: String): Option[User] = users.get().get(uuid)

  def createUser(user: UserRequest): Future[String] = {
    val uuid = UUID.randomUUID().toString
    users.alter(_.updated(uuid, User(user, uuid))).map(x => uuid)
  }

  val routes = {
    logRequest("akka-http-microservice") {
      logRequestResult("akka-http-microservice") {
        pathPrefix("order") {
          (get & path(Segment)) {
            orderId =>
              complete {
                fetchOrder(orderId).map[ToResponseMarshallable](x => x) match {
                  case Some(x) => x
                  case None => BadRequest -> s"order not found $orderId"
                }
              }
          } ~
            (post & entity(as[OrderRequest])) {
              orderRequest =>
                complete {
                  createOrder(orderRequest).map[ToResponseMarshallable](x => x)
                }
            }
        } ~
          pathPrefix("user") {
            (get & path(Segment)) {
              userId =>
                complete {
                  fetchUser(userId).map[ToResponseMarshallable](x => x) match {
                    case Some(x) => x
                    case None => BadRequest -> s"user not found $userId"
                  }
                }
            } ~
              (post & entity(as[UserRequest])) {
                userRequest =>
                  complete {
                    createUser(userRequest).map[ToResponseMarshallable](x => x)
                  }
              }
          }
      }
    }
  }
}


object AkkaHttpMicroservice extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorFlowMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
}
