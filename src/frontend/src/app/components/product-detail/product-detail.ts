import { Component, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-product-detail',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterModule],
  templateUrl: './product-detail.html'
})
export class ProductDetailComponent implements OnInit {
  product = signal<any | null>(null);
  recommendations = signal<any[]>([]);
  quantity = signal<number>(1);
  
  loading = signal<boolean>(true);
  loadingRecs = signal<boolean>(false);

  constructor(
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    public readonly apiService: ApiService
  ) {}

  ngOnInit() {
    // Watch path params
    this.route.paramMap.subscribe(params => {
      const idStr = params.get('id');
      if (idStr) {
        const id = intVal(idStr);
        const isRec = this.route.snapshot.queryParamMap.get('is_recommended') === 'true';
        this.loadProduct(id, isRec);
      }
    });
  }

  loadProduct(id: number, isRecommended: boolean) {
    this.loading.set(true);
    this.quantity.set(1);

    this.apiService.getProduct(id, isRecommended).subscribe({
      next: (data) => {
        this.product.set(data);
        this.loading.set(false);
        // Load recommendations whenever we view a product
        this.loadRecommendations();
      },
      error: (err) => {
        console.error('Failed to load product details', err);
        this.loading.set(false);
        this.router.navigate(['/']);
      }
    });
  }

  loadRecommendations() {
    this.loadingRecs.set(true);
    this.apiService.getRecommendations(6).subscribe({
      next: (data) => {
        // Exclude the current product from recommendations if it is listed
        const filtered = data.filter(p => p.id !== this.product()?.id);
        this.recommendations.set(filtered);
        this.loadingRecs.set(false);
      },
      error: (err) => {
        console.error('Failed to load recommendations', err);
        this.loadingRecs.set(false);
      }
    });
  }

  incrementQty() {
    this.quantity.update(q => q + 1);
  }

  decrementQty() {
    if (this.quantity() > 1) {
      this.quantity.update(q => q - 1);
    }
  }

  addToCart() {
    if (!this.apiService.currentUser()) {
      this.router.navigate(['/login']);
      return;
    }

    const prod = this.product();
    if (!prod) return;

    this.apiService.addToCart(prod.id, this.quantity(), false).subscribe({
      next: () => {
        alert(`${this.quantity()}x ${prod.title.substring(0, 30)}... added to cart!`);
        this.router.navigate(['/cart']);
      },
      error: (err) => {
        console.error('Error adding to cart', err);
        alert('Failed to add product to cart.');
      }
    });
  }
}

function intVal(val: string): number {
  return parseInt(val, 10);
}
