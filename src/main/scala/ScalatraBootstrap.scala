import javax.servlet.ServletContext

import com.astralync.demo.fe.KeywordsServlet
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new KeywordsServlet, "/*")
  }
}
