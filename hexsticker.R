# Hex sticker: a herringbone parquet floor with a made-up landmass inlaid in walnut, the planks
# under a bounding box lit up the way a spatial filter only reads the row groups it touches.
# Run from the package root: Rscript hexsticker.R, writes man/figures/logo.png
suppressPackageStartupMessages({library(sf); library(ggplot2)})
sf_use_s2(FALSE)

oak     <- c("#C9A16B","#BF955F","#D3AD77","#B98B56","#CEA56E","#C49A64")
walnut  <- c("#5A3B26","#6B4630","#4E3220","#63402A","#704A33")
maple   <- "#EEDFC0"
gap     <- "#4A3322"
border  <- "#3B2616"
accent  <- "#2F7FC1"
font    <- "Menlo"

hex <- (function(){a <- (90+60*(0:5))*pi/180; m <- cbind(cos(a), sin(a)); st_polygon(list(rbind(m, m[1,])))})()
hex_in <- st_buffer(hex, -0.055) # the border sits inside the hex so the canvas holds all of it
rot <- function(a){a <- a*pi/180; matrix(c(cos(a),-sin(a),sin(a),cos(a)),2)}
xform <- function(g, angle, s, off)(g*rot(angle))*s + off
rect <- function(x0,x1,y0,y1)st_polygon(list(cbind(c(x0,x1,x1,x0,x0),c(y0,y0,y1,y1,y0))))
clip <- function(x, to){
  x <- suppressWarnings(st_intersection(x, to)); x <- x[!st_is_empty(x),]
  if(all(st_dimension(x) < 2)) x else x[st_dimension(x) == 2,] # drop points and lines left where planks only touch
}

# herringbone of r:1 planks of width s, with grain lines along each plank
herringbone <- function(r=4, s=0.075, angle=45, off=c(0,0), n=40){
  planks <- list(); grain <- list()
  for(m in -ceiling(n/r):ceiling(n/r)) for(k in -n:n){
    dx <- m*r; dy <- -m*r
    h <- c(k+dx, k+r+dx, k+dy, k+1+dy); v <- c(k+dx, k+1+dx, k+1+dy, k+1+r+dy)
    planks[[length(planks)+1]] <- rect(h[1],h[2],h[3],h[4])
    planks[[length(planks)+1]] <- rect(v[1],v[2],v[3],v[4])
    gy <- h[3]+runif(3,0.15,0.85); gx <- v[1]+runif(3,0.15,0.85)
    grain[[length(grain)+1]] <- st_multilinestring(c(lapply(gy,function(y)cbind(h[1:2]+c(0.1,-0.1),y)),
                                                     lapply(gx,function(x)cbind(x,v[3:4]+c(0.1,-0.1)))))
  }
  p <- xform(st_sfc(planks), angle, s, off); g <- xform(st_sfc(grain), angle, s, off)
  keep <- lengths(st_intersects(p, hex)) > 0
  list(planks = st_sf(wood = sample(seq_along(oak), sum(keep), TRUE), geometry = p[keep]),
       grain = st_sf(geometry = g[lengths(st_intersects(g, hex)) > 0]))
}

# made-up landmass: low-frequency blob plus fractal coastline wiggle, an inlet and two islands
fake_land <- function(seed){
  set.seed(seed)
  th <- seq(0, 2*pi, length.out=721)[-721]
  r <- 1
  for(k in 2:60) r <- r + runif(1,-1,1)*(if(k<6) 0.12 else 0.35/k^1.1)*sin(k*th + runif(1,0,2*pi))
  main <- st_polygon(list(rbind(cbind(1.3*r*cos(th), r*sin(th)), c(1.3*r[1], 0))))
  a <- runif(1, 0, 2*pi)
  inlet <- st_buffer(st_linestring(cbind(1.3*cos(a)*c(1.3,0.55), sin(a)*c(1.3,0.55))), 0.12)
  land <- st_difference(st_make_valid(main), inlet)
  isl <- lapply(1:2, function(i){
    b <- a + runif(1, 1, 5); c0 <- c(1.3*cos(b), sin(b))*1.25; t2 <- th[seq(1,720,6)]
    rr <- 0.13*(1 + 0.25*sin(3*t2 + runif(1,0,6)) + 0.15*sin(7*t2 + runif(1,0,6)))*runif(1,0.6,1.2)
    st_polygon(list(rbind(cbind(c0[1]+rr*cos(t2), c0[2]+rr*sin(t2)), c(c0[1]+rr[1], c0[2]))))
  })
  st_union(st_sfc(c(list(land), isl)))
}

# scale and centre a geometry into box = c(xmin,xmax,ymin,ymax)
fit <- function(g, box){
  b <- st_bbox(g); sc <- min((box[2]-box[1])/(b$xmax-b$xmin), (box[4]-box[3])/(b$ymax-b$ymin))
  (g - c(mean(c(b$xmin,b$xmax)), mean(c(b$ymin,b$ymax))))*sc + c(mean(box[1:2]), mean(box[3:4]))
}

set.seed(42)
f <- herringbone(off=c(0.01,0.03))
land <- fit(fake_land(27), c(-0.66,0.66,-0.3,0.64))
b <- st_bbox(land); bx <- b$xmin + 0.55*(b$xmax-b$xmin); by <- b$ymin + 0.55*(b$ymax-b$ymin)
bb <- rect(bx-0.14, bx+0.14, by-0.09, by+0.09)

# oak outside the land, walnut inside, the inlaid planks under the box in the accent colour
pl <- clip(f$planks, hex_in); pl$fill <- oak[pl$wood]
inlay <- clip(pl, land); inlay$fill <- walnut[(inlay$wood-1) %% length(walnut) + 1]
inlay$fill[lengths(st_intersects(inlay, st_intersection(bb, land))) > 0] <- accent
pl <- rbind(suppressWarnings(st_difference(pl, land)), inlay)
gr <- clip(f$grain, hex_in)

top <- -0.42
band <- st_intersection(hex_in, rect(-1,1,-1,top))
p <- ggplot() +
  geom_sf(data=pl, aes(fill=I(fill)), colour=gap, linewidth=0.25) +
  geom_sf(data=gr, colour="#3A2414", alpha=0.18, linewidth=0.15) +
  geom_sf(data=land, fill=NA, colour=maple, linewidth=1.1) +
  geom_sf(data=land, fill=NA, colour=gap, linewidth=0.25) +
  geom_sf(data=st_sfc(bb), fill=NA, colour="white", linewidth=0.5, linetype="22") +
  geom_sf(data=st_sfc(band), fill=walnut[1], colour=NA) +
  geom_sf(data=st_sfc(st_intersection(hex_in, st_linestring(cbind(c(-1,1),c(top,top))))), colour=maple, linewidth=0.6) +
  annotate("text", x=0, y=(top-0.62)/2-0.11, label="r2parquet", family=font, fontface="bold", size=4.6, colour=maple) +
  geom_sf(data=st_sfc(st_difference(hex, hex_in)), fill=border, colour=NA) +
  coord_sf(xlim=c(-sqrt(3)/2, sqrt(3)/2), ylim=c(-1,1), expand=FALSE, datum=NA) +
  theme_void() + theme(plot.margin=margin(0,0,0,0), plot.background=element_blank())

dir.create("man/figures", showWarnings=FALSE, recursive=TRUE)
ggsave("man/figures/logo.png", p, width=43.9, height=50.8, units="mm", dpi=400, device=ragg::agg_png, bg="transparent")
