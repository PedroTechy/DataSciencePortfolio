import pygame as pg
from pygame.math import Vector2


class Snake(pg.sprite.Sprite):

    def __init__(self, x, y, walls, vel):
        
        super().__init__()
        self.image = pg.Surface((30, 30))
        self.image.fill(pg.Color('green'))
        self.rect = self.image.get_rect(center=(x, y))
        self.pos = Vector2(x, y)  # Position vector.
        self.dir = Vector2(0,0)
        self.vel = vel
        self.walls = walls  # A reference to the wall group.

    def update(self):
        self.pos += self.vel*self.dir
        self.wall_collisions()

    def wall_collisions(self):
        """Handle collisions with walls."""
        self.rect.centerx = self.pos.x
        for wall in pg.sprite.spritecollide(self, self.walls, False):
            if self.vel > 0:
                self.rect.right = wall.rect.left
            elif self.vel < 0:
                self.rect.left = wall.rect.right
            self.pos.x = self.rect.centerx

        self.rect.centery = self.pos.y
        for wall in pg.sprite.spritecollide(self, self.walls, False):
            if self.vel > 0:
                self.rect.bottom = wall.rect.top
            elif self.vel < 0:
                self.rect.top = wall.rect.bottom
            self.pos.y = self.rect.centery


class Wall(pg.sprite.Sprite):

    def __init__(self, x, y, w, h):
        super().__init__()
        self.image = pg.Surface((w, h))
        self.image.fill(pg.Color('sienna1'))
        self.rect = self.image.get_rect(topleft=(x, y))


def main():
    screen = pg.display.set_mode((640, 480))
    clock = pg.time.Clock()

    all_sprites = pg.sprite.Group()
    walls = pg.sprite.Group()

    wall = Wall(1, 200, 300, 30)
    wall2 = Wall(230, 70, 30, 300)
    walls.add(wall, wall2)
    all_sprites.add(wall, wall2)

    player = Snake(300, 300, walls, 20)
    all_sprites.add(player)

    done = False

    while not done:
        for event in pg.event.get():
            if event.type == pg.QUIT:
                done = True

        keys = pg.key.get_pressed()
        
        if keys[pg.K_w]:
            player.dir.y = -1 
            player.dir.x = 0
            player.dir.x = 0
        elif keys[pg.K_s]:
            player.dir.y = +1            
            player.dir.x = 0
            player.dir.x = 0
        
        if keys[pg.K_a]:
            player.dir.x = -1            
            player.dir.y = 0
            player.dir.y = 0
        elif keys[pg.K_d]:
            player.dir.x = +1            
            player.dir.y = 0
            player.dir.y = 0

        all_sprites.update()
        screen.fill((30, 30, 30))
        all_sprites.draw(screen)

        pg.display.flip()
        clock.tick(30)


if __name__ == '__main__':
    pg.init()
    main()
    pg.quit()
