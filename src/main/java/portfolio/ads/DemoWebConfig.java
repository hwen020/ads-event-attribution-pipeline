package portfolio.ads;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@Profile("public-demo")
class DemoWebConfig implements WebMvcConfigurer {
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/demo-assets/**")
            .addResourceLocations("classpath:/public-demo/");
    }
}

@Controller
@Profile("public-demo")
class DemoPageController {
    @GetMapping("/")
    String index() {
        return "forward:/demo-assets/index.html";
    }
}
