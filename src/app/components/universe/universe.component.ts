import {
  Component, ElementRef, Input, OnDestroy, AfterViewInit,
  ViewChild, signal, NgZone, output,
} from '@angular/core';
import { DatePipe } from '@angular/common';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { GitCommit } from '../../models/commit.model';

// Todas las estrellas van en un único THREE.Points (un draw call). El hover se
// resuelve en el shader comparando aIndex con uHovered, en vez de crear un mesh
// por commit. El tamaño se divide por la profundidad para simular perspectiva.
const VERT = `
  attribute float aSize;
  attribute float aIndex;
  uniform float uHovered;
  varying vec3 vColor;
  varying float vGlow;

  void main() {
    vColor = color;
    bool isHovered = abs(aIndex - uHovered) < 0.5;
    float size = isHovered ? aSize * 3.5 : aSize;
    vGlow = isHovered ? 1.0 : 0.0;
    vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
    gl_PointSize = size * (350.0 / -mvPosition.z);
    gl_Position = projectionMatrix * mvPosition;
  }
`;

// Cada punto se dibuja como un disco con núcleo brillante y halo difuso;
// descartar fuera del radio evita los cuadrados que pinta WebGL por defecto.
const FRAG = `
  varying vec3 vColor;
  varying float vGlow;

  void main() {
    vec2 coord = gl_PointCoord - vec2(0.5);
    float dist = length(coord);
    if (dist > 0.5) discard;
    float core = 1.0 - smoothstep(0.0, 0.18, dist);
    float halo = 1.0 - smoothstep(0.1, 0.5, dist);
    float alpha = core * 0.95 + halo * 0.45;
    vec3 col = vColor + vec3(core * 0.25);
    if (vGlow > 0.5) {
      col = mix(col, vec3(1.0), 0.6);
      alpha = min(alpha * 1.5, 1.0);
    }
    gl_FragColor = vec4(col, alpha);
  }
`;

const BG_VERT = `
  void main() {
    gl_PointSize = 1.0;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
  }
`;

const BG_FRAG = `
  void main() {
    float dist = length(gl_PointCoord - vec2(0.5));
    if (dist > 0.5) discard;
    gl_FragColor = vec4(1.0, 1.0, 1.0, 0.35);
  }
`;

@Component({
  selector: 'app-universe',
  standalone: true,
  imports: [DatePipe],
  template: `
    <canvas #canvas class="canvas"></canvas>

    @if (hovered()) {
      <div
        class="tooltip"
        [style.left.px]="mousePos().x + 16"
        [style.top.px]="mousePos().y - 8"
      >
        <div class="tooltip__sha">{{ hovered()!.shortSha }}</div>
        <div class="tooltip__msg">{{ hovered()!.message }}</div>
        <div class="tooltip__meta">
          {{ hovered()!.author }}
          <span class="dot">·</span>
          {{ hovered()!.date | date:'d MMM y' }}
        </div>
      </div>
    }

    <div class="hint" [class.hint--hidden]="interacted()">
      Arrastra para orbitar · Scroll para zoom
    </div>
  `,
  styleUrl: './universe.component.scss',
})
export class UniverseComponent implements AfterViewInit, OnDestroy {
  @ViewChild('canvas') canvasRef!: ElementRef<HTMLCanvasElement>;

  readonly commitClicked = output<GitCommit>();

  hovered = signal<GitCommit | null>(null);
  mousePos = signal<{ x: number; y: number }>({ x: 0, y: 0 });
  interacted = signal(false);

  private _commits: GitCommit[] = [];
  private sorted: GitCommit[] = [];

  @Input() set commits(value: GitCommit[]) {
    this._commits = value;
    if (this.scene) this.rebuildStars();
  }

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private controls!: OrbitControls;
  private starPoints: THREE.Points | null = null;
  private bgPoints: THREE.Points | null = null;
  private material!: THREE.ShaderMaterial;
  private raycaster = new THREE.Raycaster();
  private mouse = new THREE.Vector2();
  private animId = 0;
  private ro!: ResizeObserver;

  constructor(private zone: NgZone) {}

  // El bucle de render y mousemove se disparan decenas de veces por segundo:
  // fuera de la zona de Angular no provocan detección de cambios. Solo se
  // vuelve a la zona (zone.run) al actualizar signals que pinta la plantilla.
  ngAfterViewInit(): void {
    this.zone.runOutsideAngular(() => {
      this.initScene();
      this.buildBackground();
      if (this._commits.length) this.rebuildStars();
      this.startLoop();
      this.setupResize();
    });
  }

  // Three.js no libera memoria de GPU solo: sin dispose(), volver a buscar
  // otro repo acumularía contextos WebGL hasta que el navegador los tire.
  ngOnDestroy(): void {
    cancelAnimationFrame(this.animId);
    this.ro?.disconnect();
    this.renderer?.dispose();
    this.material?.dispose();
  }

  private initScene(): void {
    const canvas = this.canvasRef.nativeElement;
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;

    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x0a0a0a);
    this.scene.fog = new THREE.FogExp2(0x0a0a0a, 0.0018);

    this.camera = new THREE.PerspectiveCamera(55, w / h, 0.1, 2000);
    this.camera.position.set(0, 40, 130);

    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
    this.renderer.setSize(w, h, false);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.06;
    this.controls.minDistance = 25;
    this.controls.maxDistance = 600;
    this.controls.autoRotate = true;
    this.controls.autoRotateSpeed = 0.25;

    canvas.addEventListener('mousemove', this.onMouseMove.bind(this));
    canvas.addEventListener('click', this.onClick.bind(this));
    this.controls.addEventListener('start', () => {
      this.controls.autoRotate = false;
      this.zone.run(() => this.interacted.set(true));
    });
  }

  private buildBackground(): void {
    const count = 1800;
    const pos = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      const r = 300 + Math.random() * 700;
      const theta = Math.random() * Math.PI * 2;
      const phi = Math.acos(2 * Math.random() - 1);
      pos[i * 3] = r * Math.sin(phi) * Math.cos(theta);
      pos[i * 3 + 1] = r * Math.sin(phi) * Math.sin(theta);
      pos[i * 3 + 2] = r * Math.cos(phi);
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    this.bgPoints = new THREE.Points(
      geo,
      new THREE.ShaderMaterial({
        vertexShader: BG_VERT,
        fragmentShader: BG_FRAG,
        transparent: true,
        depthWrite: false,
      })
    );
    this.scene.add(this.bgPoints);
  }

  // Ordenar por fecha antes de posicionar: el índice en el array es el mismo
  // que usa el shader (aIndex) y el raycaster, así hover y tooltip coinciden.
  private rebuildStars(): void {
    if (this.starPoints) {
      this.scene.remove(this.starPoints);
      this.starPoints.geometry.dispose();
      this.material?.dispose();
    }

    this.sorted = [...this._commits].sort(
      (a, b) => a.date.getTime() - b.date.getTime()
    );
    const n = this.sorted.length;
    const positions = new Float32Array(n * 3);
    const colors = new Float32Array(n * 3);
    const sizes = new Float32Array(n);
    const indices = new Float32Array(n);

    for (let i = 0; i < n; i++) {
      const t = n > 1 ? i / (n - 1) : 0;
      const [x, y, z] = this.starPos(t, this.sorted[i].sha);
      positions[i * 3] = x;
      positions[i * 3 + 1] = y;
      positions[i * 3 + 2] = z;

      const c = this.ageColor(t);
      colors[i * 3] = c.r;
      colors[i * 3 + 1] = c.g;
      colors[i * 3 + 2] = c.b;

      sizes[i] = 1.8 + t * 1.2;
      indices[i] = i;
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.setAttribute('aSize', new THREE.BufferAttribute(sizes, 1));
    geo.setAttribute('aIndex', new THREE.BufferAttribute(indices, 1));

    this.material = new THREE.ShaderMaterial({
      uniforms: { uHovered: { value: -1 } },
      vertexShader: VERT,
      fragmentShader: FRAG,
      vertexColors: true,
      transparent: true,
      depthWrite: false,
    });

    this.starPoints = new THREE.Points(geo, this.material);
    this.scene.add(this.starPoints);
  }

  // Espiral cronológica: el primer commit en el centro, el último en el borde.
  // El desorden sale de un hash del SHA, así cada commit ocupa siempre el mismo
  // sitio; se reduce hacia fuera para que los brazos recientes se lean nítidos.
  private starPos(t: number, sha: string): [number, number, number] {
    const r = 8 + t * 75;
    const turns = 5;
    const theta = t * Math.PI * 2 * turns;
    const jitter = 10 * (1 - t * 0.6);
    const dx = (this.rng(sha + 'x') - 0.5) * jitter;
    const dy = (this.rng(sha + 'y') - 0.5) * jitter * 0.5;
    const dz = (this.rng(sha + 'z') - 0.5) * jitter;
    return [
      Math.cos(theta) * r + dx,
      dy,
      Math.sin(theta) * r + dz,
    ];
  }

  // Índigo (antiguo) → ámbar → blanco (reciente): la edad se lee sin tooltip.
  private ageColor(t: number): THREE.Color {
    if (t < 0.5) {
      return new THREE.Color().lerpColors(
        new THREE.Color(0x6366f1),
        new THREE.Color(0xfbbf24),
        t * 2
      );
    }
    return new THREE.Color().lerpColors(
      new THREE.Color(0xfbbf24),
      new THREE.Color(0xffffff),
      (t - 0.5) * 2
    );
  }

  // Pseudoaleatorio determinista por semilla: con Math.random() el universo
  // cambiaría de forma en cada carga del mismo repositorio.
  private rng(seed: string): number {
    let h = 0;
    for (let i = 0; i < seed.length; i++) {
      h = Math.imul(31, h) + seed.charCodeAt(i) | 0;
    }
    return Math.abs(h % 1e6) / 1e6;
  }

  private onMouseMove(e: MouseEvent): void {
    const canvas = this.canvasRef.nativeElement;
    const rect = canvas.getBoundingClientRect();
    this.mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
    this.mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
    this.zone.run(() => this.mousePos.set({ x: e.clientX, y: e.clientY }));
    this.checkHover();
  }

  private onClick(_e: MouseEvent): void {
    const h = this.hovered();
    if (h) {
      window.open(h.url, '_blank', 'noopener');
      this.zone.run(() => this.commitClicked.emit(h));
    }
  }

  // Umbral en unidades del mundo: sin margen, acertar a un punto de 2px con el
  // ratón sería casi imposible.
  private checkHover(): void {
    if (!this.starPoints) return;
    this.raycaster.params.Points = { threshold: 2.5 };
    this.raycaster.setFromCamera(this.mouse, this.camera);
    const hits = this.raycaster.intersectObject(this.starPoints);
    if (hits.length > 0) {
      const idx = hits[0].index!;
      this.material.uniforms['uHovered'].value = idx;
      this.zone.run(() => this.hovered.set(this.sorted[idx]));
    } else {
      this.material.uniforms['uHovered'].value = -1;
      this.zone.run(() => this.hovered.set(null));
    }
  }

  private startLoop(): void {
    const tick = () => {
      this.animId = requestAnimationFrame(tick);
      this.controls.update();
      this.renderer.render(this.scene, this.camera);
    };
    tick();
  }

  // ResizeObserver sobre el contenedor, no window.resize: también detecta
  // cambios de layout que no vienen de redimensionar la ventana.
  private setupResize(): void {
    this.ro = new ResizeObserver(() => {
      const canvas = this.canvasRef.nativeElement;
      const w = canvas.clientWidth;
      const h = canvas.clientHeight;
      this.camera.aspect = w / h;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(w, h, false);
    });
    this.ro.observe(this.canvasRef.nativeElement.parentElement!);
  }
}
